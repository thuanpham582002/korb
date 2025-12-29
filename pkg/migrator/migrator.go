package migrator

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"

	"beryju.org/korb/v2/pkg/strategies"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type Migrator struct {
	SourceNamespace string
	SourcePVCName   string

	DestNamespace       string
	DestPVCStorageClass string
	DestPVCSize         string
	DestPVCName         string
	DestPVCAccessModes  []string

	Force                  bool
	WaitForTempDestPVCBind bool
	TolerateAllNodes       bool
	Timeout                *time.Duration
	CopyTimeout            *time.Duration

	// Rsync-specific configuration
	RsyncLocalPath       string
	RsyncNodeName        string
	RsyncDryRun          bool
	RsyncProvisionerPath string

	// Debug-attach strategy configuration
	DebugAttachPodName       string
	DebugAttachPodSelector   string
	DebugAttachContainerName string
	DebugAttachMode          string
	DebugAttachAllPVCs       bool

	kConfig *rest.Config
	kClient *kubernetes.Clientset

	log      *log.Entry
	strategy string
	ctx      context.Context
}

func New(ctx context.Context, kubeconfigPath string, strategy string, tolerateAllNode bool) *Migrator {
	m := &Migrator{
		log:              log.WithField("component", "migrator"),
		ctx:              ctx,
		TolerateAllNodes: tolerateAllNode,
		strategy:         strategy,
	}
	if kubeconfigPath != "" {
		m.log.WithField("kubeconfig", kubeconfigPath).Debug("Created client from kubeconfig")
		cc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			&clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfigPath},
			&clientcmd.ConfigOverrides{})

		// use the current context in kubeconfig
		config, err := cc.ClientConfig()
		if err != nil {
			m.log.WithError(err).Panic("Failed to get client config")
		}
		m.kConfig = config
		ns, _, err := cc.Namespace()
		if err != nil {
			m.log.WithError(err).Panic("Failed to get current namespace")
		} else {
			m.log.WithField("namespace", ns).Debug("Got current namespace")
			m.SourceNamespace = ns
			m.DestNamespace = ns
		}
	} else {
		m.log.Panic("Kubeconfig cannot be empty")
	}

	// create the clientset
	clientset, err := kubernetes.NewForConfig(m.kConfig)
	if err != nil {
		panic(err.Error())
	}
	m.kClient = clientset
	return m
}

func (m *Migrator) Run() {
	sourcePVC, compatibleStrategies := m.Validate()
	m.log.Debug("Compatible Strategies:")
	for _, compatibleStrategy := range compatibleStrategies {
		m.log.WithField("identifier", compatibleStrategy.Identifier()).Debug(compatibleStrategy.Description())
	}
	destTemplate := m.GetDestinationPVCTemplate(sourcePVC)
	destTemplate.Name = m.DestPVCName

	var selected strategies.Strategy

	if len(compatibleStrategies) == 1 {
		m.log.Debug("Only one compatible strategy, running")
		selected = compatibleStrategies[0]
	} else {
		for _, strat := range compatibleStrategies {
			if strat.Identifier() == m.strategy {
				m.log.WithField("identifier", strat.Identifier()).Debug("User selected strategy")
				selected = strat
				break
			}
		}
	}
	if selected == nil {
		m.log.Error("No (compatible) strategy selected.")
		return
	}

	// Configure rsync strategy if selected
	if rsyncStrategy, ok := selected.(*strategies.RsyncLocalPathStrategy); ok {
		if m.RsyncLocalPath != "" {
			rsyncStrategy.SetLocalPath(m.RsyncLocalPath)
		}
		if m.RsyncProvisionerPath != "" {
			rsyncStrategy.SetProvisionerBase(m.RsyncProvisionerPath)
		}

		// Re-check compatibility after configuring rsync parameters
		ctx := strategies.MigrationContext{
			PVCControllers: []interface{}{},
			SourcePVC:      *sourcePVC,
		}
		err := selected.CompatibleWithContext(ctx)
		if err != nil {
			m.log.WithError(err).Error("Rsync strategy configuration failed")
			return
		}
	}

	// Configure debug-attach strategy if selected
	if debugAttachStrategy, ok := selected.(*strategies.DebugAttachStrategy); ok {
		if m.DebugAttachPodName != "" {
			debugAttachStrategy.SetPodName(m.DebugAttachPodName)
		}
		if m.DebugAttachPodSelector != "" {
			debugAttachStrategy.SetPodSelector(m.DebugAttachPodSelector)
		}
		if m.DebugAttachContainerName != "" {
			debugAttachStrategy.SetContainerName(m.DebugAttachContainerName)
		}
		if m.DebugAttachMode != "" {
			debugAttachStrategy.SetMode(m.DebugAttachMode)
		}
		if m.DebugAttachAllPVCs {
			debugAttachStrategy.SetAllPVCs(m.DebugAttachAllPVCs)
		}
		debugAttachStrategy.SetNamespace(m.SourceNamespace)

		// Re-check compatibility after configuring debug-attach parameters
		ctx := strategies.MigrationContext{
			PVCControllers: []interface{}{},
			SourcePVC:      *sourcePVC,
		}
		err := selected.CompatibleWithContext(ctx)
		if err != nil {
			m.log.WithError(err).Error("Debug-attach strategy configuration failed")
			return
		}
	}

	err := selected.Do(sourcePVC, destTemplate, m.WaitForTempDestPVCBind)
	if err != nil {
		m.log.WithError(err).Warning("Failed to migrate")
	}
}
