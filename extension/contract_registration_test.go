package extension_test

import (
	"testing"

	"github.com/xraph/forge/extensions/dashboard"
	fc "github.com/xraph/forge/extensions/dashboard/contract"
	"github.com/xraph/forge/extensions/dashboard/contract/dispatcher"
	forgetesting "github.com/xraph/forge/testing"

	"github.com/xraph/dispatch/extension"
	"github.com/xraph/dispatch/store/memory"
)

func TestContractContributorDiscoveredThroughRuntimeInterface(t *testing.T) {
	e := extension.New(extension.WithStore(memory.New()))
	app := forgetesting.NewTestApp("contract-app", "0.1.0")
	if err := e.Register(app); err != nil {
		t.Fatal(err)
	}
	contributor, ok := any(e).(dashboard.ContractContributorAware)
	if !ok {
		t.Fatal("dashboard cannot discover Dispatch")
	}
	reg := fc.NewRegistry()
	if err := contributor.RegisterContractContributor(dispatcher.New(nil), reg, fc.NewWardenRegistry()); err != nil {
		t.Fatal(err)
	}
	if _, ok := reg.Contributor("dispatch"); !ok {
		t.Fatal("contributor missing after runtime registration")
	}
}

func TestUninitializedContractContributorSkipsRegistration(t *testing.T) {
	e := extension.New()
	contributor, ok := any(e).(dashboard.ContractContributorAware)
	if !ok {
		t.Fatal("runtime interface missing")
	}
	reg := fc.NewRegistry()
	if err := contributor.RegisterContractContributor(dispatcher.New(nil), reg, fc.NewWardenRegistry()); err != nil {
		t.Fatal(err)
	}
	if _, ok := reg.Contributor("dispatch"); ok {
		t.Fatal("uninitialized engine registered")
	}
}
