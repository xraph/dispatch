package extension

import (
	"slices"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch/artifact/cache"
	"github.com/xraph/dispatch/resource"
)

// mergeResourceConfig folds programmatic resource settings into what YAML
// supplied, following the same rule as the rest of the extension: YAML
// wins where it said something, programmatic options fill the gaps, and
// the enable flag is an OR rather than an override.
//
// Explicit capacity merges per key rather than wholesale, so an operator
// can pin `memory` in YAML while the binary declares the `fpga` count it
// was built to talk to, and neither erases the other.
func mergeResourceConfig(yamlCfg, programmatic ResourceConfig) ResourceConfig {
	if programmatic.Enabled {
		yamlCfg.Enabled = true
	}

	if yamlCfg.CPUOvercommit <= 0 && programmatic.CPUOvercommit > 0 {
		yamlCfg.CPUOvercommit = programmatic.CPUOvercommit
	}

	if yamlCfg.MemoryFraction <= 0 && programmatic.MemoryFraction > 0 {
		yamlCfg.MemoryFraction = programmatic.MemoryFraction
	}

	if len(programmatic.Explicit) > 0 {
		merged := programmatic.Explicit.Clone()
		for k, v := range yamlCfg.Explicit {
			merged[k] = v
		}

		yamlCfg.Explicit = merged
	}

	if len(yamlCfg.CustomKeys) == 0 && len(programmatic.CustomKeys) > 0 {
		yamlCfg.CustomKeys = slices.Clone(programmatic.CustomKeys)
	}

	return yamlCfg
}

// buildResourceManager derives this worker's capacity and returns the one
// admission ledger the staging cache and the worker pool share.
//
// One instance, built here, before anything that needs it. The cache
// constructs a private manager for itself when none is supplied, so the
// mistake this function exists to make impossible is handing the cache
// one manager and the pool another: the pool's Reclaimable() would then
// be permanently zero, the disk the cache is sitting on would never be
// offered back to the dequeue budget, and the worker would go quiet
// without logging anything wrong.
//
// A nil return is the disabled path and is not an error. Every consumer
// treats nil as "no resource model", which is the behaviour that predates
// it.
func (e *Extension) buildResourceManager() resource.Manager {
	cfg := e.config.Resources
	if !cfg.Enabled {
		return nil
	}

	capacity := resource.Detect(resource.CapacityConfig{
		CPUOvercommit:  cfg.CPUOvercommit,
		MemoryFraction: cfg.MemoryFraction,
		DiskBytes:      e.stagingBudget(),
		Explicit:       cfg.Explicit.Clone(),
	})

	e.Logger().Info("dispatch: resource model enabled",
		log.Any("capacity", capacity))

	return resource.NewManager(capacity)
}

// stagingBudget is the disk capacity the shared ledger advertises.
//
// It has to come from here rather than from the cache, because
// cache.WithBudget is ignored the moment a manager is supplied — a shared
// ledger's disk capacity IS the allowance, and a second ceiling
// underneath it would only be somewhere for the two to disagree. Routing
// the configured budget in as the manager's disk capacity is what keeps
// an operator who wrote a number in the config from silently getting
// whatever Detect chose instead.
//
// Zero when there is no staging cache at all, which omits the disk key
// entirely rather than advertising capacity nothing can reclaim. An
// explicit `disk` in the resources config still overrides this, since
// Detect applies Explicit last.
func (e *Extension) stagingBudget() int64 {
	if !e.config.Artifacts.Enabled && e.artifactBackend == nil {
		return 0
	}

	if b := e.config.Artifacts.Cache.Budget; b > 0 {
		return b
	}

	return cache.DefaultBudget
}
