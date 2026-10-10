package runtime

import (
	"context"
	"errors"

	"github.com/xraph/dispatch/durable"
)

// RetirementOptions opts into persisted deployment compatibility checks.
// Enrollment remains an explicit authorized store operation.
type RetirementOptions struct {
	InstallationID string
	WriterProtocol int
}

// WorkerReadiness separates process polling from persisted deployment support.
// Query schema support does not establish a qualified host artifact or probe.
type WorkerReadiness struct {
	Worker                 WorkerStatus
	Ready                  bool
	RetirementEnabled      bool
	RetirementStatus       string
	Compatibility          durable.CompatibilityFacts
	QueryRuntimeCapability bool
}

func retirementCapabilities(store durable.Store) bool {
	_, life := store.(durable.LifecycleStore)
	_, deferral := store.(durable.WorkflowTaskDeferralStore)
	_, query := store.(durable.QueryRuntimeStore)
	_, catalog := store.(durable.NamespaceStore)
	_, outbox := store.(durable.OutboxStore)
	return life && deferral && query && catalog && outbox
}

// Readiness reads authoritative compatibility using a bounded store context.
// An unavailable observation never becomes a healthy readiness response.
func (w *Worker) Readiness(ctx context.Context) (WorkerReadiness, error) {
	result := WorkerReadiness{Worker: w.Status(), RetirementStatus: "unenrolled"}
	result.Ready = result.Worker.Ready
	_, result.QueryRuntimeCapability = w.store.(durable.QueryRuntimeStore)
	if w.options.Retirement == nil {
		return result, nil
	}
	result.RetirementEnabled = true
	result.Ready = false
	catalog, catalogOK := w.store.(durable.NamespaceStore)
	lifecycle, lifecycleOK := w.store.(durable.LifecycleStore)
	if !catalogOK || !lifecycleOK {
		result.RetirementStatus = "incompatible"
		return result, durable.ErrWriterCompatibility
	}
	target := durable.NamespaceTarget{InstallationID: w.options.Retirement.InstallationID, Namespace: w.options.Namespace}
	facts, err := storeCall(ctx, w, func(c context.Context) (durable.CompatibilityFacts, error) {
		namespace, readErr := catalog.GetNamespace(c, target.InstallationID, target.Namespace)
		if readErr != nil {
			return durable.CompatibilityFacts{}, readErr
		}
		if !namespace.RequireAudit {
			return durable.CompatibilityFacts{}, durable.ErrWriterCompatibility
		}
		return lifecycle.InspectCompatibility(c, target)
	})
	result.Worker = w.Status()
	result.Compatibility = facts
	if err != nil {
		result.RetirementStatus = "unavailable"
		if errors.Is(err, durable.ErrWriterCompatibility) {
			result.RetirementStatus = "incompatible"
		}
		return result, err
	}
	if facts.NamespaceTarget != target || !facts.Enrolled || facts.SchemaVersion != durable.RetirementSchemaVersion || facts.WriterProtocol != w.options.Retirement.WriterProtocol || facts.QueryRetentionSchemaVersion != durable.QueryRetentionSchemaVersion || facts.WorkerDrainSchemaVersion != durable.WorkerDrainSchemaVersion {
		result.RetirementStatus = "incompatible"
		if !facts.Enrolled {
			result.RetirementStatus = "unenrolled"
		}
		return result, durable.ErrWriterCompatibility
	}
	result.RetirementStatus = "ready"
	result.Ready = result.Worker.Ready
	return result, nil
}
func (w *Worker) checkRetirement(ctx context.Context) error {
	if w.options.Retirement == nil {
		return nil
	}
	_, err := w.Readiness(ctx)
	return err
}
