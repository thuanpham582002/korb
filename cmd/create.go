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
	createSource       string
	createDest         string
	createStorageClass string
	createSize         string
	createAccessMode   []string
)

var createCmd = &cobra.Command{
	Use:   "create --source <dir|file> --dest <pvc>",
	Short: "Upload local folder or archive to PVC",
	Long: `Upload local directory contents or archive files to a PVC.
Supports direct folder upload or tar/gz/zip archives.

The create operation uploads local data to a PVC, creating the PVC
if it doesn't exist.`,
	Example: `
  # Upload local folder to PVC
  korb create --source /data/myapp --dest my-pvc

  # Upload tar archive to PVC
  korb create --source backup.tar --dest my-pvc

  # Upload gz archive to PVC
  korb create --source backup.tar.gz --dest my-pvc

  # Upload zip archive to PVC
  korb create --source backup.zip --dest my-pvc -n production`,
	Args: cobra.NoArgs,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if createSource == "" {
			return fmt.Errorf("--source is required")
		}
		if createDest == "" {
			return fmt.Errorf("--dest is required")
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		if debug {
			log.SetLevel(log.DebugLevel)
		}
		runCreate(cmd.Context())
	},
}

func init() {
	// Required flags
	createCmd.Flags().StringVar(&createSource, "source", "", "Local directory or archive file (required)")
	createCmd.MarkFlagRequired("source")

	createCmd.Flags().StringVar(&createDest, "dest", "", "Destination PVC name (required)")
	createCmd.MarkFlagRequired("dest")

	// Optional flags
	createCmd.Flags().StringVar(&createStorageClass, "storage-class", "", "Storage class for new PVC")
	createCmd.Flags().StringVar(&createSize, "size", "", "PVC size (e.g., 10Gi, 100Gi)")
	createCmd.Flags().StringSliceVar(&createAccessMode, "access-mode", []string{}, "Access mode(s) (ReadWriteOnce, ReadWriteMany, etc.)")
}

func runCreate(ctx context.Context) {
	log.WithFields(log.Fields{
		"source":    createSource,
		"dest":      createDest,
		"namespace": namespace,
	}).Info("Starting create operation")

	// Check if source exists
	if _, err := os.Stat(createSource); os.IsNotExist(err) {
		log.Fatalf("Source '%s' does not exist", createSource)
	}

	// Detect source type (directory or file)
	sourceInfo, err := os.Stat(createSource)
	if err != nil {
		log.WithError(err).Fatalf("Failed to stat source '%s'", createSource)
	}

	isDir := sourceInfo.IsDir()
	log.WithFields(log.Fields{
		"source": createSource,
		"type":   map[bool]string{true: "directory", false: "file"}[isDir],
	}).Debug("Detected source type")

	// Create migrator with create strategy
	// For create, we use "import" strategy but with local source
	m := migrator.New(ctx, kubeConfig, "import", false)
	m.Timeout = parseTimeout()
	m.SourceNamespace = namespace
	m.DestNamespace = namespace

	// Set destination PVC options
	m.DestPVCName = createDest
	m.DestPVCStorageClass = createStorageClass
	m.DestPVCSize = createSize
	m.DestPVCAccessModes = createAccessMode

	// Set source file/dir path
	m.SourcePVCName = createSource // Used as file path for create

	// Run create
	m.Run()
	log.Info("Create completed successfully")
}
