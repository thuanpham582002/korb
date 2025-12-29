package cmd

import (
	"context"
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"beryju.org/korb/v2/pkg/migrator"
)

var (
	exportSource  string
	exportFile    string
	exportType    string
	exportAllPVCs bool
)

var exportCmd = &cobra.Command{
	Use:   "export --source <pvc|pod> --file <archive>",
	Short: "Export PVC to archive file",
	Long: `Export PVC contents to a tar archive. Can export single PVC or
all PVCs from a pod. Supports compression formats.

The export operation creates a tar archive of the PVC contents.`,
	Example: `
  # Export PVC to tar
  korb export --source my-pvc --file backup.tar

  # Export with compression
  korb export --source my-pvc --file backup.tar.gz --type gz -n production

  # Export all PVCs from pod
  korb export --source my-pod --all-pvcs --file app-backup.tar`,
	Args: cobra.NoArgs,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if exportSource == "" {
			return fmt.Errorf("--source is required")
		}
		if exportFile == "" {
			return fmt.Errorf("--file is required")
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		if debug {
			log.SetLevel(log.DebugLevel)
		}
		runExport(cmd.Context())
	},
}

func init() {
	// Required flags
	exportCmd.Flags().StringVar(&exportSource, "source", "", "Source PVC or pod name (required)")
	exportCmd.MarkFlagRequired("source")

	exportCmd.Flags().StringVar(&exportFile, "file", "", "Output archive file (required)")
	exportCmd.MarkFlagRequired("file")

	// Optional flags
	exportCmd.Flags().StringVar(&exportType, "type", "tar", "Archive type (tar|gz|zip)")
	exportCmd.Flags().BoolVar(&exportAllPVCs, "all-pvcs", false, "Export all PVCs from source pod")
}

func runExport(ctx context.Context) {
	log.WithFields(log.Fields{
		"source":    exportSource,
		"file":      exportFile,
		"type":      exportType,
		"allPVCs":   exportAllPVCs,
		"namespace": namespace,
	}).Info("Starting export operation")

	// For export, we use the export strategy
	m := migrator.New(ctx, kubeConfig, "export", false)
	m.Timeout = parseTimeout()
	m.SourceNamespace = namespace

	if exportAllPVCs {
		// Source is a pod, export all its PVCs
		log.WithField("pod", exportSource).Info("Exporting all PVCs from pod")
		log.Info("Running in [EPHEMERAL CONTAINER ATTACH] mode - attaching to existing pod")
		m.SourcePVCName = exportSource
		m.DebugAttachPodName = exportSource
		m.DebugAttachMode = "export"
		m.DebugAttachAllPVCs = true
	} else {
		// Source is a PVC
		log.WithField("pvc", exportSource).Info("Exporting PVC")
		log.Info("Running in [JOB] mode - creating mover job")
		m.SourcePVCName = exportSource
	}

	// Check if source exists
	sourceType := detectResourceType(ctx, exportSource, namespace)
	if sourceType == "unknown" {
		log.Fatalf("Source '%s' not found as PVC or pod in namespace '%s'", exportSource, namespace)
	}

	// Run export
	m.Run()
	log.Info("Export completed successfully")
}
