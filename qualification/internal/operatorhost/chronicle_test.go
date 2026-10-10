package operatorhost

import (
	"testing"

	"github.com/xraph/dispatch/durable/delivery/ecosystem"
	"github.com/xraph/dispatch/store/memory"
)

func TestChroniclePublisherRequiresPersistedBinding(t *testing.T) {
	c := newConfiguredCommandClient(t, memory.New(), &LifecycleOptions{InstanceID: "publisher-binding", SkipSampleExecutions: true})
	namespace, err := c.host.Store.GetNamespace(t.Context(), "operator-host", "production")
	if err != nil {
		t.Fatal(err)
	}
	binding := ecosystem.Binding{Producer: "lifecycle", InstallationID: "operator-host", Namespace: "production", AppID: namespace.AppID, TenantID: "foreign"}
	if stop, startErr := c.host.StartChroniclePublisher(t.Context(), binding, "http://127.0.0.1:1/accept", "private"); startErr == nil || stop != nil {
		t.Fatal("publisher accepted foreign persisted ownership")
	}
}
