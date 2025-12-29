package cmd

import (
	"context"

	log "github.com/sirupsen/logrus"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// detectResourceType determines if source is PVC or pod
func detectResourceType(ctx context.Context, source, ns string) string {
	// Create Kubernetes client
	config, err := clientcmd.BuildConfigFromFlags("", kubeConfig)
	if err != nil {
		log.WithError(err).Warn("Failed to create Kubernetes config")
		return "unknown"
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.WithError(err).Warn("Failed to create Kubernetes client")
		return "unknown"
	}

	// Try to get as PVC first
	_, err = clientset.CoreV1().PersistentVolumeClaims(ns).Get(ctx, source, metav1.GetOptions{})
	if err == nil {
		return "pvc"
	}

	// Try to get as pod
	_, err = clientset.CoreV1().Pods(ns).Get(ctx, source, metav1.GetOptions{})
	if err == nil {
		return "pod"
	}

	log.WithField("source", source).Warn("Could not detect resource type")
	return "unknown"
}
