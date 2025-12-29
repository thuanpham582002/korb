package cmd

import (
	"context"
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"beryju.org/korb/v2/pkg/migrator"
)

var (
	importFile         string
	importDest         string
	importStorageClass string
	importSize         string
	importAccessMode   []string
)

var importCmd = &cobra.Command{
	Use:   "import --file <archive> --dest <pvc>",
	Short: "Import archive to PVC",
	Long: `Import data from tar archive to a PVC. Creates new PVC if it doesn't exist.

The import operation extracts data from an archive and writes it to the PVC.`,
	Example: `
  # Import to existing PVC
  korb import --file backup.tar --dest my-pvc

  # Import to new PVC with specific storage class
  korb import --file backup.tar --dest new-pvc --storage-class fast-ssd -n production`,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if importFile == "" {
			return fmt.Errorf("--file is required")
		}
		if importDest == "" {
			return fmt.Errorf("--dest is required")
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		if debug {
			log.SetLevel(log.DebugLevel)
		}
		runImport(cmd.Context())
	},
}

func init() {
	// Required flags
	importCmd.Flags().StringVar(&importFile, "file", "", "Archive file to import (required)")
	importCmd.MarkFlagRequired("file")

	importCmd.Flags().StringVar(&importDest, "dest", "", "Destination PVC name (required)")
	importCmd.MarkFlagRequired("dest")

	// Optional flags
	importCmd.Flags().StringVar(&importStorageClass, "storage-class", "", "Storage class for new PVC")
	importCmd.Flags().StringVar(&importSize, "size", "", "PVC size (e.g., 10Gi, 100Gi)")
	importCmd.Flags().StringSliceVar(&importAccessMode, "access-mode", []string{}, "Access mode(s) (ReadWriteOnce, ReadWriteMany, etc.)")
}

func runImport(ctx context.Context) {
	log.WithFields(log.Fields{
		"file":      importFile,
		"dest":      importDest,
		"namespace": namespace,
	}).Info("Starting import operation")

	// Check if archive file exists
	if _, err := os.Stat(importFile); os.IsNotExist(err) {
		log.Fatalf("Archive file '%s' does not exist", importFile)
	}

	// Create migrator with import strategy
	m := migrator.New(ctx, kubeConfig, "import", false)
	m.Timeout = parseTimeout()
	m.SourceNamespace = namespace
	m.DestNamespace = namespace

	// Set destination PVC options
	m.DestPVCName = importDest
	m.DestPVCStorageClass = importStorageClass
	m.DestPVCSize = importSize
	m.DestPVCAccessModes = importAccessMode

	// Set import file
	m.SourcePVCName = importFile // Used as file path for import

	// Run import
	m.Run()
	log.Info("Import completed successfully")
}
