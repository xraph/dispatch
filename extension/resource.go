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

	staging := e.stagingBudget()

	e.warnOnDiskOverride(staging)

	capacity := resource.Detect(resource.CapacityConfig{
		CPUOvercommit:  cfg.CPUOvercommit,
		MemoryFraction: cfg.MemoryFraction,
		DiskBytes:      staging,
		Explicit:       cfg.Explicit.Clone(),
	})

	e.Logger().Info("dispatch: resource model enabled",
		log.Any("capacity", capacity))

	return resource.NewManager(capacity)
}

// warnOnDiskOverride reports two config keys setting the same number with
// the less obvious one winning.
//
// resources.explicit.disk is not merely what this worker advertises. The
// cache reads its budget straight off the shared ledger's disk capacity
// (Cache.Budget), so an explicit value becomes the ceiling the cache
// evicts against and therefore the ceiling it WRITES against. Pin it
// above the volume and the worker fills the disk rather than evicting,
// and the first symptom is ENOSPC in a job that has nothing to do with
// caching.
//
// The precedence is deliberate — explicit means explicit, and an operator
// who wants a ledger disk figure that differs from the cache allowance
// has legitimate reasons — so this warns rather than fails. What it
// refuses to do is let the disagreement stay silent.
func (e *Extension) warnOnDiskOverride(staging int64) {
	explicit, conflict := diskOverride(e.config.Resources.Explicit, staging)
	if !conflict {
		return
	}

	e.Logger().Warn("dispatch: resources.explicit.disk overrides the staging cache budget",
		log.Int64("explicit_disk", explicit),
		log.Int64("cache_budget", staging),
		log.String("effect", "the staging cache evicts against explicit_disk, so a value "+
			"above the volume's free space fills the disk instead of reclaiming"))
}

// diskOverride reports whether an explicit disk capacity disagrees with
// the staging budget it is about to replace, and what it is.
//
// Nothing to say when there is no staging cache to disagree with, when
// the operator set only one of the two, or when they set both to the same
// number.
func diskOverride(explicit resource.Set, staging int64) (int64, bool) {
	v, set := explicit[resource.Disk]
	if !set || staging <= 0 || v == staging {
		return 0, false
	}

	return v, true
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
//
// The artifactStore check is the one that is easy to miss: init only
// builds the plane when the dispatcher's store implements artifact.Store,
// so `artifacts.enabled: true` on a store that does not is a configured
// plane that never exists. Testing the config alone would advertise
// 20 GiB of disk with no cache behind it and no reclaimer registered for
// it — the exact invariant TestNoArtifactPlaneOmitsDisk pins. This
// requires the store to be resolved before the ledger is built, which is
// why init does that first.
func (e *Extension) stagingBudget() int64 {
	if e.artifactStore == nil {
		return 0
	}

	if !e.config.Artifacts.Enabled && e.artifactBackend == nil {
		return 0
	}

	if b := e.config.Artifacts.Cache.Budget; b > 0 {
		return b
	}

	return cache.DefaultBudget
}
