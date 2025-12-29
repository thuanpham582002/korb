package cmd

import (
	"context"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"beryju.org/korb/v2/pkg/migrator"
)

var (
	cloneSource       string
	cloneDest         string
	cloneStorageClass string
	cloneAllPVCs      bool
	cloneStrategy     string
	cloneTolerateAll  bool
	cloneSize         string
	cloneAccessMode   []string
	cloneSkipWaitBind bool
	cloneForce        bool
	cloneCopyTimeout  string
)

var cloneCmd = &cobra.Command{
	Use:   "clone --source <pvc|pod>",
	Short: "Clone PVC to new PVC or storage class",
	Long: `Clone a PVC to a new PVC with different storage class, size, or name.
Can also clone all PVCs from a pod at once.

The clone operation copies data from source PVC to destination PVC
without modifying the source PVC.`,
	Example: `
  # Clone PVC to new storage class
  korb clone --source my-pvc --dest my-pvc-fast --storage-class fast-ssd

  # Clone all PVCs from a pod
  korb clone --source my-app-pod --all-pvcs --storage-class fast-ssd -n production

  # Clone with specific options
  korb clone --source my-pvc --dest my-pvc-clone --storage-class local-path --size 10Gi`,
	Args: cobra.NoArgs,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if cloneSource == "" {
			return fmt.Errorf("--source is required")
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		if debug {
			log.SetLevel(log.DebugLevel)
		}
		runClone(cmd.Context())
	},
}

func init() {
	// Required flags
	cloneCmd.Flags().StringVar(&cloneSource, "source", "", "Source PVC or pod name (required)")
	cloneCmd.MarkFlagRequired("source")

	// Destination flags
	cloneCmd.Flags().StringVar(&cloneDest, "dest", "", "Destination PVC name")
	cloneCmd.Flags().StringVar(&cloneStorageClass, "storage-class", "", "Destination storage class")
	cloneCmd.Flags().StringVar(&cloneSize, "size", "", "New PVC size (e.g., 10Gi, 100Gi)")
	cloneCmd.Flags().StringSliceVar(&cloneAccessMode, "access-mode", []string{}, "Access mode(s) (ReadWriteOnce, ReadWriteMany, etc.)")

	// Behavior flags
	cloneCmd.Flags().BoolVar(&cloneAllPVCs, "all-pvcs", false, "Clone all PVCs from source pod")
	cloneCmd.Flags().StringVar(&cloneStrategy, "strategy", "auto", "Migration strategy (auto|clone|copy-twice-name|debug-attach)")
	cloneCmd.Flags().BoolVar(&cloneTolerateAll, "tolerate-all-nodes", false, "Allow scheduling on any node despite taints")
	cloneCmd.Flags().BoolVar(&cloneSkipWaitBind, "skip-pvc-bind-wait", false, "Skip waiting for PVC to be bound")
	cloneCmd.Flags().BoolVar(&cloneForce, "force", false, "Ignore validation warnings")
	cloneCmd.Flags().StringVar(&cloneCopyTimeout, "copy-timeout", "", "Override copy timeout (default: 60s/GB)")
}

func runClone(ctx context.Context) {
	log.WithFields(log.Fields{
		"source":     cloneSource,
		"dest":       cloneDest,
		"allPVCs":    cloneAllPVCs,
		"strategy":   cloneStrategy,
		"namespace":  namespace,
	}).Info("Starting clone operation")

	// Show execution mode
	if cloneStrategy == "debug-attach" {
		log.Info("Running in [EPHEMERAL CONTAINER ATTACH] mode - attaching to existing pod")
	} else {
		log.Info("Running in [JOB] mode - creating mover job")
	}

	// Parse timeouts
	var timeout *time.Duration
	if t := parseTimeout(); t != nil {
		timeout = t
	}

	var copyTimeout *time.Duration
	if cloneCopyTimeout != "" {
		ct, err := time.ParseDuration(cloneCopyTimeout)
		if err != nil {
			log.WithError(err).Fatal("Failed to parse copy-timeout")
		}
		copyTimeout = &ct
	}

	// Create migrator
	m := migrator.New(ctx, kubeConfig, cloneStrategy, cloneTolerateAll)
	m.Force = cloneForce
	m.WaitForTempDestPVCBind = !cloneSkipWaitBind
	m.Timeout = timeout
	m.CopyTimeout = copyTimeout
	m.SourceNamespace = namespace
	m.DestNamespace = namespace

	// Set destination PVC options
	m.DestPVCName = cloneDest
	m.DestPVCStorageClass = cloneStorageClass
	m.DestPVCSize = cloneSize
	m.DestPVCAccessModes = cloneAccessMode

	// Set debug-attach options if strategy selected
	if cloneStrategy == "debug-attach" {
		m.DebugAttachPodName = cloneSource // For pod-based source
		m.DebugAttachMode = "clone"
		m.DebugAttachAllPVCs = cloneAllPVCs
	}

	// Determine source type and run
	if cloneAllPVCs {
		// Source is a pod, clone all its PVCs
		log.WithField("pod", cloneSource).Info("Cloning all PVCs from pod")
		m.SourcePVCName = cloneSource // Will be treated as pod name
	} else {
		// Source is a PVC
		log.WithField("pvc", cloneSource).Info("Cloning PVC")
		m.SourcePVCName = cloneSource
	}

	// Check if source is PVC or pod
	sourceType := detectResourceType(ctx, cloneSource, namespace)
	if sourceType == "pod" && !cloneAllPVCs {
		log.Fatal("source is a pod, please specify --all-pvcs to clone all PVCs from the pod")
	}

	m.Run()
	log.Info("Clone completed successfully")
}
