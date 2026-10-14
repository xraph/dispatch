//go:build linux

package managedhost

import (
	"testing"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/qualification/writerfloor/launcher"
)

func registryHost(runtime string) *RuntimeHost {
	return &RuntimeHost{slots: map[string]*serviceSlot{"fixed": {startup: launcher.ServiceStartup{ServicePlan: launcher.ServicePlan{Installation: "installation", Namespace: "namespace", Build: "build", RuntimeID: runtime}}}}}
}
func TestNativeRuntimeRegistryIsFixedBoundedAndUnique(t *testing.T) {
	first, second := registryHost("first"), registryHost("second")
	registry, err := nativeRuntimeRegistry([]*RuntimeHost{first, second})
	if err != nil || len(registry) != 2 {
		t.Fatalf("two hosts: %v", err)
	}
	target := durable.QueryRuntimeTarget{BuildTarget: durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: "installation", Namespace: "namespace"}, BuildID: "build"}, RuntimeID: "second"}
	if registry[target] != second {
		t.Fatal("survivor routed to wrong configured host")
	}
	if _, err = nativeRuntimeRegistry([]*RuntimeHost{first, registryHost("first")}); err == nil {
		t.Fatal("ambiguous target accepted")
	}
	if _, err = nativeRuntimeRegistry(make([]*RuntimeHost, 17)); err == nil {
		t.Fatal("unbounded registry accepted")
	}
	if _, err = nativeRuntimeRegistry([]*RuntimeHost{nil}); err == nil {
		t.Fatal("nil host accepted")
	}
	host := &NativeGuardHost{runtimes: registry}
	if _, err = host.runtimeFor(target); err == nil {
		t.Fatal("unenrolled process accepted")
	}
	target.RuntimeID = "caller-selected"
	if _, err = host.runtimeFor(target); err == nil {
		t.Fatal("unknown process accepted")
	}
}
