// flag: rsync-local-path
// Behavior: Rsync local directory into a new PVC

package strategies

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"beryju.org/korb/v2/pkg/mover"
)

type RsyncLocalPathStrategy struct {
	BaseStrategy

	LocalPath       string
	DestPVC         *v1.PersistentVolumeClaim
	tempMover       *mover.MoverJob
	NodeName        string
	ProvisionerBase string
}

func NewRsyncLocalPathStrategy(b BaseStrategy) *RsyncLocalPathStrategy {
	s := &RsyncLocalPathStrategy{
		BaseStrategy:    b,
		ProvisionerBase: "/opt/local-path-provisioner",
	}
	s.log = s.log.WithField("strategy", s.Identifier())
	return s
}

func (r *RsyncLocalPathStrategy) Identifier() string {
	return "rsync-local-path"
}

func (r *RsyncLocalPathStrategy) CompatibleWithContext(ctx MigrationContext) error {
	// Check if source path exists
	info, err := os.Stat(r.LocalPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("local path '%s' does not exist", r.LocalPath)
		}
		return fmt.Errorf("failed to access local path '%s': %w", r.LocalPath, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("local path '%s' is not a directory", r.LocalPath)
	}

	// Check if we can read the directory
	file, err := os.Open(r.LocalPath)
	if err != nil {
		return fmt.Errorf("cannot read local path '%s': %w", r.LocalPath, err)
	}
	err = file.Close()
	if err != nil {
		return fmt.Errorf("failed to close directory: %w", err)
	}

	return nil
}

func (r *RsyncLocalPathStrategy) Description() string {
	return "Rync local directory into a new PVC using local-path provisioner."
}

func (r *RsyncLocalPathStrategy) Do(sourcePVC *v1.PersistentVolumeClaim, destTemplate *v1.PersistentVolumeClaim, WaitForTempDestPVCBind bool) error {
	r.log.Info("Starting rsync from local path to PVC")
	r.log.Warning("This strategy requires local-path provisioner and access to the host filesystem.")

	// Step 1: Create destination PVC
	r.log.Debug("Creating destination PVC")
	r.DestPVC = destTemplate
	r.DestPVC.Spec = sourcePVC.Spec

	// Apply PVC template modifications if provided
	if destTemplate != nil {
		r.applyPVCTemplate()
	}

	createdPVC, err := r.kClient.CoreV1().PersistentVolumeClaims(destTemplate.Namespace).Create(r.ctx, r.DestPVC, metav1.CreateOptions{})
	if err != nil {
		r.log.WithError(err).Error("Failed to create destination PVC")
		return err
	}
	r.DestPVC = createdPVC

	r.log.Info("Waiting for PVC to be bound...")
	err = r.waitForPVCBinding()
	if err != nil {
		r.log.WithError(err).Error("Failed to wait for PVC binding")
		return r.cleanupPVC()
	}

	// Step 2: Determine the node and local path
	err = r.determineNodeAndPath()
	if err != nil {
		r.log.WithError(err).Error("Failed to determine node and path")
		return r.cleanupPVC()
	}

	// Step 3: Create mover pod to access the PVC
	r.log.Debug("Creating mover job to access PVC")
	r.tempMover = mover.NewMoverJob(r.ctx, r.kClient, mover.MoverTypeSleep, r.tolerateAllNodes)
	r.tempMover.Namespace = r.DestPVC.Namespace
	r.tempMover.SourceVolume = r.DestPVC
	r.tempMover.Name = fmt.Sprintf("korb-rsync-job-%s", r.DestPVC.UID)

	// Add node selector to ensure pod runs on the same node as the PVC
	pod := r.tempMover.Start().WithNodeSelector(r.NodeName).WaitForRunning(r.timeout)
	if pod == nil {
		r.log.Error("Failed to start mover pod")
		return r.cleanupPVC()
	}

	// Step 4: Rync the data
	r.log.Info("Starting rsync operation...")
	err = r.rsyncToLocalPath(*pod)
	if err != nil {
		r.log.WithError(err).Error("Rsync operation failed")
		return r.cleanupPVCAndMover()
	}

	r.log.Info("Rsync operation completed successfully")
	return r.cleanupMover()
}

func (r *RsyncLocalPathStrategy) applyPVCTemplate() {
	if r.DestPVC.Spec.StorageClassName != nil {
		r.DestPVC.Spec.StorageClassName = r.DestPVC.Spec.StorageClassName
	}
	if r.DestPVC.Spec.Resources.Requests != nil {
		r.DestPVC.Spec.Resources.Requests = r.DestPVC.Spec.Resources.Requests
	}
	if len(r.DestPVC.Spec.AccessModes) > 0 {
		r.DestPVC.Spec.AccessModes = r.DestPVC.Spec.AccessModes
	}
}

func (r *RsyncLocalPathStrategy) waitForPVCBinding() error {
	// Wait for PVC to be bound and get the PV name
	pvc, err := r.kClient.CoreV1().PersistentVolumeClaims(r.DestPVC.Namespace).Get(r.ctx, r.DestPVC.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	// Poll for binding
	for i := 0; i < 60; i++ { // Max 10 minutes
		if pvc.Status.Phase == v1.ClaimBound && pvc.Spec.VolumeName != "" {
			return nil
		}
		pvc, err = r.kClient.CoreV1().PersistentVolumeClaims(r.DestPVC.Namespace).Get(r.ctx, r.DestPVC.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
	}
	return fmt.Errorf("timeout waiting for PVC %s to bind", r.DestPVC.Name)
}

func (r *RsyncLocalPathStrategy) determineNodeAndPath() error {
	// Get the PV to determine where the local-path provisioner created the directory
	pvc, err := r.kClient.CoreV1().PersistentVolumeClaims(r.DestPVC.Namespace).Get(r.ctx, r.DestPVC.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	// Get the PersistentVolume
	pv, err := r.kClient.CoreV1().PersistentVolumes().Get(r.ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	// Extract node information from PV's node affinity
	if pv.Spec.NodeAffinity != nil && pv.Spec.NodeAffinity.Required != nil {
		for _, term := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
			for _, expr := range term.MatchExpressions {
				if expr.Key == "kubernetes.io/hostname" && len(expr.Values) > 0 {
					r.NodeName = expr.Values[0]
					r.log.WithField("node", r.NodeName).Debug("Determined target node")
					break
				}
			}
		}
	}

	if r.NodeName == "" {
		return fmt.Errorf("could not determine target node from PV")
	}

	return nil
}

func (r *RsyncLocalPathStrategy) rsyncToLocalPath(pod v1.Pod) error {
	// Find the local path provisioner directory for this PVC
	// The directory format is typically: pvc-{uuid}_{namespace}_{pvc-name}

	// Try to list the directory to find the PVC's directory
	cmd := []string{
		"bash",
		"-c",
		fmt.Sprintf("ls %s/ | grep '%s_%s_%s'", r.ProvisionerBase, r.DestPVC.UID, r.DestPVC.Namespace, r.DestPVC.Name),
	}

	output, err := r.tempMover.ExecCapture(pod, r.kConfig, cmd)
	if err != nil {
		// Try alternative approach: just list all PVC directories and find ours
		cmd := []string{
			"bash",
			"-c",
			fmt.Sprintf("ls %s/ | grep '%s' | head -1", r.ProvisionerBase, r.DestPVC.Name),
		}
		output, err = r.tempMover.ExecCapture(pod, r.kConfig, cmd)
		if err != nil {
			return fmt.Errorf("failed to find PVC directory: %w", err)
		}
	}

	pvcDir := strings.TrimSpace(output)
	if pvcDir == "" {
		return fmt.Errorf("could not find PVC directory in local path provisioner")
	}

	pvcPath := filepath.Join(r.ProvisionerBase, pvcDir)
	r.log.WithField("path", pvcPath).Debug("Found PVC directory")

	// First, copy local files to a temporary location in the pod
	tempDest := "/tmp/rsync-source"

	// Use the existing CopyInto method to copy local files to the pod temp directory
	// For now, we'll create a tar approach, but this could be optimized for rsync
	localTarPath := r.createTempTar()
	defer func() {
		err := os.Remove(localTarPath)
		if err != nil {
			r.log.WithError(err).Warning("Failed to remove temporary tar file")
		}
	}()

	// Upload and extract the tar to temp location
	cmd = []string{
		"bash",
		"-c",
		fmt.Sprintf("mkdir -p %s && cd %s && tar xvzf -", tempDest, tempDest),
	}

	file, err := os.Open(localTarPath)
	if err != nil {
		return fmt.Errorf("failed to open temporary tar file: %w", err)
	}
	defer func() {
		err := file.Close()
		if err != nil {
			r.log.WithError(err).Warning("Failed to close temporary tar file")
		}
	}()

	err = r.tempMover.Exec(pod, r.kConfig, cmd, file, os.Stdout)
	if err != nil {
		return fmt.Errorf("failed to upload local files: %w", err)
	}

	// Now rsync from the temp location to the PVC
	cmd = []string{
		"bash",
		"-c",
		fmt.Sprintf("rsync -aHAX --delete %s/ %s/", tempDest, mover.SourceMount),
	}

	err = r.tempMover.Exec(pod, r.kConfig, cmd, nil, os.Stdout)
	if err != nil {
		return fmt.Errorf("rsync operation failed: %w", err)
	}

	// Cleanup temp directory
	cmd = []string{
		"bash",
		"-c",
		fmt.Sprintf("rm -rf %s", tempDest),
	}
	r.tempMover.Exec(pod, r.kConfig, cmd, nil, os.Stdout)

	return nil
}

func (r *RsyncLocalPathStrategy) createTempTar() string {
	tempFile := filepath.Join(os.TempDir(), fmt.Sprintf("korb-rsync-%s.tar", r.DestPVC.Name))

	// Create tar of the local directory
	cmd := exec.Command("tar", "czf", tempFile, "-C", r.LocalPath, ".")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		r.log.WithError(err).Error("Failed to create temporary tar file")
		return ""
	}

	r.log.WithField("file", tempFile).Debug("Created temporary tar file")
	return tempFile
}

func (r *RsyncLocalPathStrategy) cleanupPVC() error {
	if r.DestPVC != nil {
		r.log.Info("Cleaning up created PVC...")
		err := r.kClient.CoreV1().PersistentVolumeClaims(r.DestPVC.Namespace).Delete(r.ctx, r.DestPVC.Name, metav1.DeleteOptions{})
		if err != nil {
			r.log.WithError(err).Warning("Failed to cleanup PVC")
			return err
		}
	}
	return nil
}

func (r *RsyncLocalPathStrategy) cleanupMover() error {
	r.log.Info("Cleaning up mover...")
	if r.tempMover != nil {
		return r.tempMover.Cleanup()
	}
	return nil
}

func (r *RsyncLocalPathStrategy) cleanupPVCAndMover() error {
	err1 := r.cleanupMover()
	err2 := r.cleanupPVC()
	if err1 != nil {
		return err1
	}
	return err2
}

func (r *RsyncLocalPathStrategy) Cleanup() error {
	return r.cleanupPVCAndMover()
}

// SetLocalPath sets the source local directory path
func (r *RsyncLocalPathStrategy) SetLocalPath(path string) {
	r.LocalPath = path
}

// SetProvisionerBase sets the base path for the local-path provisioner
func (r *RsyncLocalPathStrategy) SetProvisionerBase(path string) {
	r.ProvisionerBase = path
}