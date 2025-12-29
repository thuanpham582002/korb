package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	log "github.com/sirupsen/logrus"

	"beryju.org/korb/v2/pkg/config"

	"github.com/spf13/cobra"
	"k8s.io/client-go/util/homedir"
)

var (
	// Global flags (inherited by all subcommands)
	kubeConfig string
	namespace  string
	debug      bool
	timeout    string
)

var Version string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "korb",
	Short: "Kubernetes PVC migration and backup tool",
	Long: `Korb is a tool for migrating, backing up, and restoring
Kubernetes Persistent Volume Claims (PVCs) with support for
multiple storage classes and backup formats.`,
	Version: Version,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	ctx, cncl := signal.NotifyContext(context.Background(), os.Kill, os.Interrupt)
	defer cncl()

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	log.SetLevel(log.InfoLevel)

	// PersistentFlags are inherited by all subcommands
	if home := homedir.HomeDir(); home != "" {
		rootCmd.PersistentFlags().StringVar(&kubeConfig, "kube-config", filepath.Join(home, ".kube", "config"), "Kubernetes config file")
	} else {
		rootCmd.PersistentFlags().StringVar(&kubeConfig, "kube-config", "", "Kubernetes config file")
	}
	rootCmd.PersistentFlags().StringVarP(&namespace, "namespace", "n", "default", "Kubernetes namespace")
	rootCmd.PersistentFlags().BoolVar(&debug, "debug", false, "Enable debug logging")
	rootCmd.PersistentFlags().StringVar(&timeout, "timeout", "", "Operation timeout (e.g., 5m, 1h)")
	rootCmd.PersistentFlags().StringVar(&config.ContainerImage, "container-image", config.ContainerImage, "Image to use for mover jobs")

	// Add subcommands
	rootCmd.AddCommand(cloneCmd)
	rootCmd.AddCommand(exportCmd)
	rootCmd.AddCommand(importCmd)
	rootCmd.AddCommand(createCmd)
}

func parseTimeout() *time.Duration {
	if timeout == "" {
		return nil
	}
	t, err := time.ParseDuration(timeout)
	if err != nil {
		log.WithError(err).Panic("Failed to parse timeout")
	}
	return &t
}
