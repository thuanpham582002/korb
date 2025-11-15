// flag: clone
// Behavior: Clone data from source PVC to destination PVC without modifying the source

package strategies

import (
	"context"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	"beryju.org/korb/v2/pkg/mover"
)

type CloneStrategy struct {
	BaseStrategy

	DestPVC *v1.PersistentVolumeClaim

	tempMover *mover.MoverJob

	MoveTimeout time.Duration

	WaitForTempDestPVCBind bool
}

func NewCloneStrategy(b BaseStrategy) *CloneStrategy {
	s := &CloneStrategy{
		BaseStrategy: b,
	}
	s.log = s.log.WithField("strategy", s.Identifier())
	return s
}

func (c *CloneStrategy) Identifier() string {
	return "clone"
}

func (c *CloneStrategy) CompatibleWithContext(ctx MigrationContext) error {
	// Clone strategy is always compatible - it's the simplest option
	return nil
}

func (c *CloneStrategy) Description() string {
	return "Clone data from source PVC to destination PVC without modifying the source PVC."
}

func (c *CloneStrategy) Do(sourcePVC *v1.PersistentVolumeClaim, destTemplate *v1.PersistentVolumeClaim, WaitForTempDestPVCBind bool) error {
	c.setTimeout(destTemplate)
	c.log.Info("Starting clone from source PVC to destination PVC")

	// Create destination PVC
	c.log.Debug("creating destination PVC")
	destInst, err := c.kClient.CoreV1().PersistentVolumeClaims(destTemplate.ObjectMeta.Namespace).Create(c.ctx, destTemplate, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	c.DestPVC = destInst

	if c.WaitForTempDestPVCBind {
		err = c.waitForBound(destInst)
		if err != nil {
			c.log.WithError(err).Warning("Waiting for destination PVC to be bound failed")
			return c.Cleanup()
		}
	} else {
		c.log.Debug("skipping waiting for destination PVC to be bound")
	}

	// Start mover job to copy data from source to destination
	c.log.Debug("starting mover job to clone data")
	c.tempMover = mover.NewMoverJob(c.ctx, c.kClient, mover.MoverTypeSync, c.tolerateAllNodes)
	c.tempMover.Namespace = destTemplate.Namespace
	c.tempMover.SourceVolume = sourcePVC
	c.tempMover.DestVolume = c.DestPVC
	c.tempMover.Name = "korb-clone-" + string(sourcePVC.UID)

	err = c.tempMover.Start().Wait(c.timeout, c.MoveTimeout)
	if err != nil {
		c.log.WithError(err).Warning("Failed to clone data")
		return c.Cleanup()
	}

	c.log.Info("Clone completed successfully - both source and destination PVCs exist")
	return c.Cleanup()
}

func (c *CloneStrategy) Cleanup() error {
	c.log.Info("Cleaning up mover job...")
	if c.tempMover != nil {
		return c.tempMover.Cleanup()
	}
	return nil
}

func (c *CloneStrategy) setTimeout(pvc *v1.PersistentVolumeClaim) {
	if c.copyTimeout != nil {
		c.MoveTimeout = *c.copyTimeout
	} else {
		sizeInByes, _ := pvc.Spec.Resources.Requests.Storage().AsInt64()
		sizeInMB := float64(sizeInByes) / 1024 / 1024
		c.MoveTimeout = time.Duration(sizeInMB*(60.0/1024)) * time.Second
	}
	c.log.WithField("timeout", c.MoveTimeout).Debug("Set timeout from PVC size")
}

func (c *CloneStrategy) waitForBound(p *v1.PersistentVolumeClaim) error {
	return wait.PollUntilContextTimeout(c.ctx, 2*time.Second, c.timeout, true, func(ctx context.Context) (bool, error) {
		pvc, err := c.kClient.CoreV1().PersistentVolumeClaims(p.ObjectMeta.Namespace).Get(ctx, p.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if pvc.Status.Phase != v1.ClaimBound {
			c.log.WithField("pvc-name", p.ObjectMeta.Name).Debug("Destination PVC not bound yet, retrying")
			return false, nil
		}
		return true, nil
	})
}