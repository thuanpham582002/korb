// flag: debug-attach
// Behavior: Attach ephemeral debug container to running pod with PVC mount for live data copy/export

package strategies

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"

	"beryju.org/korb/v2/pkg/config"
	"beryju.org/korb/v2/pkg/mover"
)

type DebugAttachStrategy struct {
	BaseStrategy

	// User-specified targeting
	PodName         string
	PodSelector     string
	ContainerName   string
	Namespace       string
	Mode            string // "auto", "export", "clone", "import"
	AllPVCs         bool

	// Internal state
	TargetPod      *v1.Pod
	DebugContainer *v1.EphemeralContainer
	DestPVC        *v1.PersistentVolumeClaim

	// Temporary resource tracking for WaitForFirstConsumer
	tempPods []string
	tempPVCs []*v1.PersistentVolumeClaim

	// Lifecycle
	MoveTimeout time.Duration
}

func NewDebugAttachStrategy(b BaseStrategy) *DebugAttachStrategy {
	s := &DebugAttachStrategy{
		BaseStrategy: b,
		Mode:         "auto",
	}
	s.log = s.log.WithField("strategy", s.Identifier())
	return s
}

func (d *DebugAttachStrategy) Identifier() string {
	return "debug-attach"
}

func (d *DebugAttachStrategy) Description() string {
	return "Attach ephemeral debug container to running pod with PVC mount for live data copy/export."
}

func (d *DebugAttachStrategy) CompatibleWithContext(ctx MigrationContext) error {
	// Debug-attach is compatible when at least one pod using the PVC exists
	if d.PodName == "" && d.PodSelector == "" && !d.AllPVCs {
		// Auto-discovery mode - check if any pods use this PVC
		pods, err := d.kClient.CoreV1().Pods(d.Namespace).List(d.ctx, metav1.ListOptions{})
		if err != nil {
			return fmt.Errorf("failed to list pods: %w", err)
		}

		found := false
		for _, pod := range pods.Items {
			for _, volume := range pod.Spec.Volumes {
				if volume.PersistentVolumeClaim != nil &&
					volume.PersistentVolumeClaim.ClaimName == ctx.SourcePVC.Name {
					found = true
					break
				}
			}
			if found {
				break
			}
		}

		if !found {
			return fmt.Errorf("no running pods found using PVC %s", ctx.SourcePVC.Name)
		}
	}
	return nil
}

// Setter methods for configuration
func (d *DebugAttachStrategy) SetPodName(name string) { d.PodName = name }
func (d *DebugAttachStrategy) SetPodSelector(selector string) { d.PodSelector = selector }
func (d *DebugAttachStrategy) SetContainerName(name string) { d.ContainerName = name }
func (d *DebugAttachStrategy) SetMode(mode string) { d.Mode = mode }
func (d *DebugAttachStrategy) SetAllPVCs(all bool) { d.AllPVCs = all }
func (d *DebugAttachStrategy) SetNamespace(ns string) { d.Namespace = ns }

func (d *DebugAttachStrategy) Do(sourcePVC *v1.PersistentVolumeClaim, destTemplate *v1.PersistentVolumeClaim, waitForTempDestPVCBind bool) error {
	d.setTimeout(sourcePVC)

	// Phase 1: Discover target pod
	err := d.discoverTargetPod(sourcePVC)
	if err != nil {
		return fmt.Errorf("failed to discover target pod: %w", err)
	}

	// Phase 2: Determine which PVCs to process
	var pvcNames []string
	if d.AllPVCs {
		pvcNames, _ = d.findAllPVCsInPod()
		d.log.WithField("count", len(pvcNames)).Info("Processing all PVCs in pod")
	} else {
		pvcNames = []string{sourcePVC.Name}
	}

	// Phase 3: Store destination template for clone mode
	// (PVCs will be created in executeMultiPVCClone with 1-1 mover job mapping)
	if destTemplate != nil && (d.Mode == "auto" || d.Mode == "clone") {
		d.DestPVC = destTemplate
		d.log.WithField("destTemplate", destTemplate.Name).Debug("Stored destination template for clone mode")
	}

	// Phase 4: Attach ephemeral container
	err = d.attachEphemeralContainer(sourcePVC, pvcNames)
	if err != nil {
		return fmt.Errorf("failed to attach ephemeral container: %w", err)
	}

	// Phase 5: Execute data copy
	err = d.executeDataCopy(pvcNames)
	if err != nil {
		return fmt.Errorf("failed to copy data: %w", err)
	}

	// Phase 6: Cleanup ephemeral container
	return d.cleanupEphemeralContainer()
}

func (d *DebugAttachStrategy) discoverTargetPod(sourcePVC *v1.PersistentVolumeClaim) error {
	d.log.WithField("pvc", sourcePVC.Name).Debug("Discovering target pod")

	// Method 1: Explicit pod name
	if d.PodName != "" {
		pod, err := d.kClient.CoreV1().Pods(d.Namespace).Get(d.ctx, d.PodName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get pod %s: %w", d.PodName, err)
		}
		d.TargetPod = pod
		d.log.WithField("pod", pod.Name).Info("Using explicitly specified pod")
		return nil
	}

	// Method 2: Pod selector
	if d.PodSelector != "" {
		pods, err := d.kClient.CoreV1().Pods(d.Namespace).List(d.ctx, metav1.ListOptions{
			LabelSelector: d.PodSelector,
		})
		if err != nil {
			return fmt.Errorf("failed to list pods with selector %s: %w", d.PodSelector, err)
		}
		if len(pods.Items) == 0 {
			return fmt.Errorf("no pods found with selector %s", d.PodSelector)
		}
		// Use first running pod
		for _, pod := range pods.Items {
			if pod.Status.Phase == v1.PodRunning {
				d.TargetPod = &pod
				d.log.WithField("pod", pod.Name).Info("Using pod from selector")
				return nil
			}
		}
		return fmt.Errorf("no running pods found with selector %s", d.PodSelector)
	}

	// Method 3: Auto-discovery from PVC
	return d.autoDiscoverPodFromPVC(sourcePVC)
}

func (d *DebugAttachStrategy) autoDiscoverPodFromPVC(sourcePVC *v1.PersistentVolumeClaim) error {
	// List all pods in namespace and check if they use this PVC
	pods, err := d.kClient.CoreV1().Pods(d.Namespace).List(d.ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list pods: %w", err)
	}

	var candidates []*v1.Pod
	for _, pod := range pods.Items {
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil &&
				volume.PersistentVolumeClaim.ClaimName == sourcePVC.Name {
				candidates = append(candidates, &pod)
				break
			}
		}
	}

	if len(candidates) == 0 {
		return fmt.Errorf("no pods found using PVC %s", sourcePVC.Name)
	}

	// Prefer running pods
	for _, pod := range candidates {
		if pod.Status.Phase == v1.PodRunning {
			d.TargetPod = pod
			d.log.WithField("pod", pod.Name).Info("Auto-discovered running pod")
			return nil
		}
	}

	// Fallback to first candidate
	d.TargetPod = candidates[0]
	d.log.WithField("pod", d.TargetPod.Name).WithField("phase", d.TargetPod.Status.Phase).
		Warning("Using non-running pod")
	return nil
}

func (d *DebugAttachStrategy) findAllPVCsInPod() ([]string, map[string]string) {
	pvcNames := []string{}
	pvcToVolume := map[string]string{}

	for _, volume := range d.TargetPod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil {
			pvcNames = append(pvcNames, volume.PersistentVolumeClaim.ClaimName)
			pvcToVolume[volume.PersistentVolumeClaim.ClaimName] = volume.Name
		}
	}

	return pvcNames, pvcToVolume
}

func (d *DebugAttachStrategy) findPVCVolumeInPod(pvcName string) string {
	for _, volume := range d.TargetPod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil &&
			volume.PersistentVolumeClaim.ClaimName == pvcName {
			return volume.Name
		}
	}
	return ""
}

func (d *DebugAttachStrategy) attachEphemeralContainer(sourcePVC *v1.PersistentVolumeClaim, pvcNames []string) error {
	d.log.WithField("pod", d.TargetPod.Name).Debug("Attaching ephemeral container")

	// Determine target container name
	targetContainer := d.ContainerName
	if targetContainer == "" && len(d.TargetPod.Spec.Containers) > 0 {
		targetContainer = d.TargetPod.Spec.Containers[0].Name
	}

	// Build volume mounts for all PVCs
	volumeMounts := []v1.VolumeMount{}
	pvcToMountPath := map[string]string{}

	for i, pvcName := range pvcNames {
		volumeName := d.findPVCVolumeInPod(pvcName)
		if volumeName == "" {
			return fmt.Errorf("PVC %s not found in pod spec", pvcName)
		}

		// Create unique mount path for each PVC
		mountPath := fmt.Sprintf("%s-%d", mover.SourceMount, i)
		volumeMounts = append(volumeMounts, v1.VolumeMount{
			Name:      volumeName,
			MountPath: mountPath,
			ReadOnly:  true,
		})
		pvcToMountPath[pvcName] = mountPath
	}

	ephemeral := &v1.EphemeralContainer{
		EphemeralContainerCommon: v1.EphemeralContainerCommon{
			Name:         "korb-debug-" + uuid.New().String()[:8],
			Image:        config.ContainerImage,
			Command:      []string{"/bin/sh"},
			Args:         []string{"-c", "sleep 3600"}, // Keep alive for operations
			VolumeMounts: volumeMounts,
			Stdin:        true,
			TTY:          true,
		},
		TargetContainerName: targetContainer,
	}

	// Store PVC to mount path mapping for later use
	d.DebugContainer = ephemeral

	// Patch pod with ephemeral container
	patchData := map[string]interface{}{
		"spec": map[string]interface{}{
			"ephemeralContainers": []v1.EphemeralContainer{*ephemeral},
		},
	}

	patchBytes, err := json.Marshal(patchData)
	if err != nil {
		return fmt.Errorf("failed to marshal patch: %w", err)
	}

	_, err = d.kClient.CoreV1().Pods(d.Namespace).Patch(
		d.ctx,
		d.TargetPod.Name,
		types.StrategicMergePatchType,
		patchBytes,
		metav1.PatchOptions{},
		"ephemeralcontainers", // Subresource
	)
	if err != nil {
		return fmt.Errorf("failed to add ephemeral container: %w", err)
	}

	d.DebugContainer = ephemeral

	// Wait for ephemeral container to be running
	return d.waitForEphemeralContainer()
}

func (d *DebugAttachStrategy) waitForEphemeralContainer() error {
	d.log.Debug("Waiting for ephemeral container to be ready")

	return wait.PollUntilContextTimeout(d.ctx, 2*time.Second, d.timeout, true, func(ctx context.Context) (bool, error) {
		pod, err := d.kClient.CoreV1().Pods(d.Namespace).Get(d.ctx, d.TargetPod.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		// Check ephemeral container status
		for _, ec := range pod.Spec.EphemeralContainers {
			if ec.Name == d.DebugContainer.Name {
				// Find container status
				for _, cs := range pod.Status.EphemeralContainerStatuses {
					if cs.Name == d.DebugContainer.Name {
						if cs.State.Running != nil {
							d.log.Debug("Ephemeral container is running")
							d.TargetPod = pod // Update pod reference
							return true, nil
						}
						if cs.State.Terminated != nil {
							return false, fmt.Errorf("ephemeral container terminated: %s", cs.State.Terminated.Reason)
						}
					}
				}
			}
		}

		d.log.WithField("phase", pod.Status.Phase).Debug("Waiting for ephemeral container...")
		return false, nil
	})
}

func (d *DebugAttachStrategy) executeDataCopy(pvcNames []string) error {
	determineMode := func() string {
		if d.Mode != "auto" {
			return d.Mode
		}
		if d.DestPVC != nil || d.AllPVCs {
			return "clone"
		}
		return "export"
	}

	mode := determineMode()
	d.log.WithField("mode", mode).Info("Starting data copy via ephemeral container")

	switch mode {
	case "export":
		return d.executeMultiPVCExport(pvcNames)
	case "clone":
		return d.executeMultiPVCClone(pvcNames)
	default:
		return fmt.Errorf("unsupported mode: %s", mode)
	}
}

func (d *DebugAttachStrategy) createEphemeralDestPod(pvc *v1.PersistentVolumeClaim) (string, error) {
	podName := "korb-dest-" + uuid.New().String()[:8]

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: pvc.Namespace,
		},
		Spec: v1.PodSpec{
			// Use same node as source pod for local-path storage
			NodeName: d.TargetPod.Spec.NodeName,
			Volumes: []v1.Volume{
				{
					Name: "dest-volume",
					VolumeSource: v1.VolumeSource{
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvc.Name,
						},
					},
				},
			},
			Containers: []v1.Container{
				{
					Name:    "dest",
					Image:   config.ContainerImage,
					Command: []string{"sh", "-c", "sleep 3600"},
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      "dest-volume",
							MountPath: "/data",
						},
					},
				},
			},
			RestartPolicy: v1.RestartPolicyNever,
		},
	}

	// Copy tolerations from source pod
	if len(d.TargetPod.Spec.Tolerations) > 0 {
		pod.Spec.Tolerations = d.TargetPod.Spec.Tolerations
	}

	createdPod, err := d.kClient.CoreV1().Pods(pvc.Namespace).Create(d.ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return "", err
	}

	d.log.WithField("pod", createdPod.Name).WithField("pvc", pvc.Name).Info("Created ephemeral destination pod")
	d.tempPods = append(d.tempPods, podName)
	return podName, nil
}

func (d *DebugAttachStrategy) executeTarRsyncToPod(srcMountPath string, destPodName string) error {
	d.log.WithField("src", srcMountPath).WithField("dest", destPodName).Info("Starting tar pipe transfer")

	// Create tar pipe command from source ephemeral container
	cmd := []string{
		"sh", "-c",
		fmt.Sprintf("tar cf - %s | kubectl exec -i %s -n %s -- tar xf - -C /data",
			srcMountPath, destPodName, d.Namespace),
	}

	// Execute using existing ExecInContainer method
	moverJob := mover.NewMoverJob(d.ctx, d.kClient, mover.MoverTypeSleep, false)
	moverJob.Namespace = d.Namespace

	if err := moverJob.ExecInContainer(*d.TargetPod, d.kConfig, d.DebugContainer.Name, cmd, nil, os.Stdout); err != nil {
		return fmt.Errorf("tar pipe transfer failed: %w", err)
	}

	d.log.WithField("src", srcMountPath).WithField("dest", destPodName).Info("Transfer completed")
	return nil
}

func (d *DebugAttachStrategy) waitForPodReady(podName string) error {
	d.log.WithField("pod", podName).Debug("Waiting for pod to be ready")

	return wait.PollUntilContextTimeout(d.ctx, 2*time.Second, d.timeout, true, func(ctx context.Context) (bool, error) {
		pod, err := d.kClient.CoreV1().Pods(d.Namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if pod.Status.Phase == v1.PodRunning {
			for _, cs := range pod.Status.ContainerStatuses {
				if cs.Ready {
					d.log.WithField("pod", podName).Debug("Pod is ready")
					return true, nil
				}
			}
		}
		return false, nil
	})
}

func (d *DebugAttachStrategy) executeMultiPVCClone(pvcNames []string) error {
	d.log.Info("Executing multi-PVC clone with ephemeral pods")

	// Phase 1: Create destination PVCs and ephemeral pods (1-1 mapping)
	destPods := []string{}

	for _, pvcName := range pvcNames {
		srcPVC, err := d.kClient.CoreV1().PersistentVolumeClaims(d.Namespace).Get(d.ctx, pvcName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get source PVC %s: %w", pvcName, err)
		}

		// Create destination PVC
		var destName string
		if d.AllPVCs {
			destName = d.DestPVC.Name + "-" + pvcName
		} else {
			destName = d.DestPVC.Name
		}

		destInst := srcPVC.DeepCopy()
		destInst.Name = destName
		if d.DestPVC.Spec.StorageClassName != nil {
			destInst.Spec.StorageClassName = d.DestPVC.Spec.StorageClassName
		}
		// Critical: Clear all binding metadata to prevent ClaimLost
		destInst.Spec.VolumeName = ""
		destInst.Status = v1.PersistentVolumeClaimStatus{}
		destInst.ResourceVersion = ""
		destInst.UID = ""
		// Clear only PV binding annotations, preserve user annotations
		if destInst.Annotations != nil {
			for key := range destInst.Annotations {
				if strings.HasPrefix(key, "pv.kubernetes.io/") || strings.HasPrefix(key, "volume.beta.kubernetes.io/") {
					delete(destInst.Annotations, key)
				}
			}
		}

		d.log.WithField("source", pvcName).WithField("dest", destName).Info("Creating destination PVC")
		destPVC, err := d.kClient.CoreV1().PersistentVolumeClaims(d.Namespace).Create(d.ctx, destInst, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("failed to create destination PVC %s: %w", destName, err)
		}

		// Create ephemeral destination pod (triggers WaitForFirstConsumer)
		destPodName, err := d.createEphemeralDestPod(destPVC)
		if err != nil {
			return fmt.Errorf("failed to create dest pod %s: %w", destName, err)
		}
		destPods = append(destPods, destPodName)

		// Wait for dest pod to be ready
		if err := d.waitForPodReady(destPodName); err != nil {
			return fmt.Errorf("dest pod %s not ready: %w", destPodName, err)
		}
	}

	// Phase 2: Execute tar pipe transfers from source ephemeral container
	for i, pvcName := range pvcNames {
		srcMountPath := fmt.Sprintf("%s-%d", mover.SourceMount, i)
		destPodName := destPods[i]

		if err := d.executeTarRsyncToPod(srcMountPath, destPodName); err != nil {
			d.log.WithError(err).WithField("pvc", pvcName).Warning("Transfer failed, continuing")
			continue
		}

		d.log.WithField("source", pvcName).WithField("dest", destPodName).Info("PVC cloned successfully")
	}

	// Phase 3: Cleanup ephemeral destination pods
	for _, podName := range destPods {
		d.log.WithField("pod", podName).Debug("Deleting ephemeral destination pod")
		d.kClient.CoreV1().Pods(d.Namespace).Delete(d.ctx, podName, metav1.DeleteOptions{})
	}

	d.log.Info("Multi-PVC clone completed")
	return nil
}

func (d *DebugAttachStrategy) executeMultiPVCExport(pvcNames []string) error {
	d.log.Info("Executing multi-PVC export")

	// Build a map of PVC name to mount path
	pvcToMountPath := map[string]string{}
	for i, pvcName := range pvcNames {
		pvcToMountPath[pvcName] = fmt.Sprintf("%s-%d", mover.SourceMount, i)
	}

	for _, pvcName := range pvcNames {
		tarFile := pvcName + ".tar"
		mountPath := pvcToMountPath[pvcName]

		d.log.WithField("pvc", pvcName).WithField("file", tarFile).WithField("mount", mountPath).Info("Exporting PVC")

		// Create tar in ephemeral container
		cmd := []string{
			"tar",
			"cf",
			"/tmp/" + tarFile,
			"-C",
			mountPath,
			".",
		}

		err := d.execInEphemeralContainer(cmd)
		if err != nil {
			d.log.WithError(err).WithField("pvc", pvcName).Warning("Failed to create tar")
			continue
		}

		// Download tar file from ephemeral container
		err = d.downloadFromEphemeralContainer("/tmp/"+tarFile, tarFile)
		if err != nil {
			d.log.WithError(err).WithField("pvc", pvcName).Warning("Failed to download tar")
			continue
		}

		d.log.WithField("pvc", pvcName).Info("PVC exported successfully")
	}

	return nil
}

func (d *DebugAttachStrategy) execInEphemeralContainer(cmd []string) error {
	d.log.WithField("cmd", cmd).Debug("Executing command in ephemeral container")

	// Create a simple mover job to use ExecInContainer
	moverJob := mover.NewMoverJob(d.ctx, d.kClient, mover.MoverTypeSleep, false)
	moverJob.Namespace = d.Namespace

	err := moverJob.ExecInContainer(*d.TargetPod, d.kConfig, d.DebugContainer.Name, cmd, nil, os.Stdout)
	if err != nil {
		return fmt.Errorf("exec command failed: %w", err)
	}

	return nil
}

func (d *DebugAttachStrategy) downloadFromEphemeralContainer(remotePath, localPath string) error {
	d.log.WithField("remote", remotePath).WithField("local", localPath).Debug("Downloading file")

	// Use kubectl log-like approach to cat the file
	moverJob := mover.NewMoverJob(d.ctx, d.kClient, mover.MoverTypeSleep, false)
	moverJob.Namespace = d.Namespace

	localFile, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("failed to create local file: %w", err)
	}
	defer localFile.Close()

	// Cat the remote file and pipe to local file
	cmd := []string{"cat", remotePath}
	err = moverJob.ExecInContainer(*d.TargetPod, d.kConfig, d.DebugContainer.Name, cmd, nil, localFile)
	if err != nil {
		return fmt.Errorf("failed to download file: %w", err)
	}

	d.log.WithField("size", localFile.Name()).Info("Downloaded file")
	return nil
}

func (d *DebugAttachStrategy) cleanupEphemeralContainer() error {
	d.log.Debug("Cleaning up ephemeral container")

	// Remove ephemeral container by patching with empty array
	patchData := map[string]interface{}{
		"spec": map[string]interface{}{
			"ephemeralContainers": []v1.EphemeralContainer{},
		},
	}

	patchBytes, err := json.Marshal(patchData)
	if err != nil {
		return fmt.Errorf("failed to marshal cleanup patch: %w", err)
	}

	_, err = d.kClient.CoreV1().Pods(d.Namespace).Patch(
		d.ctx,
		d.TargetPod.Name,
		types.StrategicMergePatchType,
		patchBytes,
		metav1.PatchOptions{},
		"ephemeralcontainers",
	)
	if err != nil {
		d.log.WithError(err).Warning("Failed to remove ephemeral container (may have already been removed)")
		return nil // Don't fail on cleanup error
	}

	d.log.Info("Ephemeral container removed")
	return nil
}

func (d *DebugAttachStrategy) Cleanup() error {
	d.log.Info("Cleaning up temporary resources")

	// Clean up ephemeral container
	d.cleanupEphemeralContainer()

	// Clean up temporary consumer pods
	for _, podName := range d.tempPods {
		d.log.WithField("pod", podName).Debug("Deleting temporary consumer pod")
		err := d.kClient.CoreV1().Pods(d.Namespace).Delete(d.ctx, podName, metav1.DeleteOptions{})
		if err != nil {
			d.log.WithError(err).WithField("pod", podName).Warning("Failed to delete temporary consumer pod")
		}
	}

	// Clean up temporary PVCs (if any created during testing)
	for _, pvc := range d.tempPVCs {
		d.log.WithField("pvc", pvc.Name).Debug("Deleting temporary PVC")
		err := d.kClient.CoreV1().PersistentVolumeClaims(d.Namespace).Delete(d.ctx, pvc.Name, metav1.DeleteOptions{})
		if err != nil {
			d.log.WithError(err).WithField("pvc", pvc.Name).Warning("Failed to delete temporary PVC")
		}
	}

	return nil
}

func (d *DebugAttachStrategy) setTimeout(pvc *v1.PersistentVolumeClaim) {
	if d.copyTimeout != nil {
		d.MoveTimeout = *d.copyTimeout
	} else {
		sizeInByes, _ := pvc.Spec.Resources.Requests.Storage().AsInt64()
		sizeInMB := float64(sizeInByes) / 1024 / 1024
		d.MoveTimeout = time.Duration(sizeInMB*(60.0/1024)) * time.Second
	}
	d.log.WithField("timeout", d.MoveTimeout).Debug("Set timeout from PVC size")
}

// isWaitForFirstConsumerStorageClass detects if PVC uses WaitForFirstConsumer binding mode
func (d *DebugAttachStrategy) isWaitForFirstConsumerStorageClass(pvc *v1.PersistentVolumeClaim) bool {
	if pvc.Spec.StorageClassName == nil {
		return false
	}

	sc, err := d.kClient.StorageV1().StorageClasses().Get(d.ctx, *pvc.Spec.StorageClassName, metav1.GetOptions{})
	if err != nil {
		d.log.WithError(err).WithField("storageClass", *pvc.Spec.StorageClassName).Debug("Failed to get storage class")
		return false
	}

	isWFFC := sc.VolumeBindingMode != nil && *sc.VolumeBindingMode == storagev1.VolumeBindingWaitForFirstConsumer
	d.log.WithField("pvc", pvc.Name).WithField("storageClass", *pvc.Spec.StorageClassName).WithField("isWaitForFirstConsumer", isWFFC).Debug("Detected storage class binding mode")
	return isWFFC
}

// createTemporaryConsumerPod creates a temporary pod to trigger WaitForFirstConsumer PVC binding
func (d *DebugAttachStrategy) createTemporaryConsumerPod(pvc *v1.PersistentVolumeClaim) (string, error) {
	podName := "korb-temp-consumer-" + uuid.New().String()[:8]

	// Use node name from target pod if available (critical for local-path storage)
	nodeName := d.TargetPod.Spec.NodeName
	if nodeName == "" && len(d.TargetPod.Spec.NodeSelector) > 0 {
		// Fallback to node selector if nodeName is not set
		d.log.WithField("nodeSelector", d.TargetPod.Spec.NodeSelector).Debug("Using node selector instead of node name")
	}

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: pvc.Namespace,
		},
		Spec: v1.PodSpec{
			NodeName: nodeName,
			Volumes: []v1.Volume{
				{
					Name: "pvc-volume",
					VolumeSource: v1.VolumeSource{
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvc.Name,
						},
					},
				},
			},
			Containers: []v1.Container{
				{
					Name:    "consumer",
					Image:   "busybox:latest",
					Command: []string{"sh", "-c", "echo 'Triggering PVC binding' && sleep 3600"},
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      "pvc-volume",
							MountPath: "/data",
						},
					},
				},
			},
			RestartPolicy: v1.RestartPolicyNever,
		},
	}

	// Copy tolerations from target pod if available
	if len(d.TargetPod.Spec.Tolerations) > 0 {
		pod.Spec.Tolerations = d.TargetPod.Spec.Tolerations
	}

	d.log.WithField("pod", podName).WithField("pvc", pvc.Name).WithField("node", nodeName).Info("Creating temporary consumer pod")
	createdPod, err := d.kClient.CoreV1().Pods(pvc.Namespace).Create(d.ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to create temporary consumer pod: %w", err)
	}

	d.log.WithField("pod", createdPod.Name).WithField("pvc", pvc.Name).Info("Created temporary consumer pod")
	d.tempPods = append(d.tempPods, podName)
	return podName, nil
}

// waitForPVCBinding waits for a PVC to be bound
func (d *DebugAttachStrategy) waitForPVCBinding(pvc *v1.PersistentVolumeClaim) error {
	d.log.WithField("pvc", pvc.Name).Debug("Waiting for PVC to be bound")

	bindingTimeout := d.timeout * 2
	return wait.PollUntilContextTimeout(d.ctx, 2*time.Second, bindingTimeout, true, func(ctx context.Context) (bool, error) {
		currentPVC, err := d.kClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		if currentPVC.Status.Phase == v1.ClaimBound {
			d.log.WithField("pvc", pvc.Name).WithField("volume", currentPVC.Spec.VolumeName).Info("PVC is bound")
			return true, nil
		}

		d.log.WithField("pvc", pvc.Name).WithField("phase", currentPVC.Status.Phase).Debug("PVC not bound yet")
		return false, nil
	})
}
