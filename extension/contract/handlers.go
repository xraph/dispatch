package contract

import (
	"context"
	"slices"
	"strings"
	"time"

	fc "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/dispatch/resource"
)

type HandlerInput struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}
type HandlerRow struct {
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	Versions   []int  `json:"versions"`
	InputCount int    `json:"inputCount"`
}
type HandlerArtifactInput struct {
	Name     string `json:"name"`
	Required bool   `json:"required"`
	MaxSize  int64  `json:"maxSize"`
	Mode     string `json:"mode"`
}
type ExecutionPolicy struct {
	Level          string   `json:"level"`
	GracePeriod    Duration `json:"gracePeriod"`
	AllowDowngrade bool     `json:"allowDowngrade"`
	Image          *string  `json:"image"`
}
type JobHandlerDetail struct {
	Inputs            []HandlerArtifactInput `json:"inputs"`
	Resources         resource.Set           `json:"resources"`
	ResourceLimits    resource.Set           `json:"resourceLimits"`
	ResourceClass     *string                `json:"resourceClass"`
	ResourceFunction  bool                   `json:"resourceFunction"`
	LeaseTTL          *Duration              `json:"leaseTtl"`
	EffectiveLeaseTTL Duration               `json:"effectiveLeaseTtl"`
	Execution         ExecutionPolicy        `json:"execution"`
}
type HandlerDetail struct {
	HandlerRow
	Job  *JobHandlerDetail `json:"job"`
	AsOf string            `json:"asOf"`
}

func handlersListHandler(deps Deps) func(context.Context, EmptyInput, fc.Principal) (Page[HandlerRow], error) {
	return handle(deps, "handlers.list", false, func(_ context.Context, _ EmptyInput, _ fc.Principal) (Page[HandlerRow], error) {
		registry := deps.Engine.Registry()
		workflows := deps.Engine.WorkflowRunner().Registry()
		jobNames, workflowNames := registry.Names(), workflows.Names()
		rows := make([]HandlerRow, 0, len(jobNames)+len(workflowNames))
		for _, name := range jobNames {
			rows = append(rows, HandlerRow{Kind: "job", Name: name, Versions: []int{}, InputCount: len(registry.Inputs(name))})
		}
		for _, name := range workflowNames {
			rows = append(rows, HandlerRow{Kind: "workflow", Name: name, Versions: workflows.Versions(name)})
		}
		slices.SortFunc(rows, func(a, b HandlerRow) int {
			if c := strings.Compare(a.Kind, b.Kind); c != 0 {
				return c
			}
			return strings.Compare(a.Name, b.Name)
		})
		return newPage(rows, "", true, time.Now()), nil
	})
}
func handlersGetHandler(deps Deps) func(context.Context, HandlerInput, fc.Principal) (HandlerDetail, error) {
	return handle(deps, "handlers.get", false, func(_ context.Context, input HandlerInput, _ fc.Principal) (HandlerDetail, error) {
		if strings.TrimSpace(input.Name) == "" {
			return HandlerDetail{}, badRequest("name must identify a handler")
		}
		out := HandlerDetail{HandlerRow: HandlerRow{Kind: input.Kind, Name: input.Name, Versions: []int{}}, AsOf: time.Now().UTC().Format(time.RFC3339Nano)}
		switch input.Kind {
		case "workflow":
			registry := deps.Engine.WorkflowRunner().Registry()
			if _, ok := registry.Get(input.Name); !ok {
				return HandlerDetail{}, notFound("workflow definition not found")
			}
			out.Versions = registry.Versions(input.Name)
		case "job":
			registry := deps.Engine.Registry()
			if _, ok := registry.Get(input.Name); !ok {
				return HandlerDetail{}, notFound("job handler not found")
			}
			decl, policy := registry.Resources(input.Name), registry.Policy(input.Name)
			detail := &JobHandlerDetail{Inputs: []HandlerArtifactInput{}, Resources: resourceValues(decl.Requests), ResourceLimits: resourceValues(decl.Limits),
				ResourceClass: nullable(decl.Class), ResourceFunction: decl.Func != nil, EffectiveLeaseTTL: duration(deps.Engine.Inspect().Pool.DefaultLeaseTTL),
				Execution: ExecutionPolicy{Level: policy.Level.String(), GracePeriod: duration(policy.GracePeriod), AllowDowngrade: policy.AllowDowngrade, Image: nullable(policy.Image)}}
			if ttl := registry.LeaseTTL(input.Name); ttl > 0 {
				value := duration(ttl)
				detail.LeaseTTL = &value
				detail.EffectiveLeaseTTL = value
			}
			for _, spec := range registry.Inputs(input.Name) {
				detail.Inputs = append(detail.Inputs, HandlerArtifactInput{Name: spec.Name, Required: spec.Required, MaxSize: spec.MaxSize, Mode: spec.Mode.String()})
			}
			out.InputCount = len(detail.Inputs)
			out.Job = detail
		default:
			return HandlerDetail{}, badRequest("kind must be job or workflow")
		}
		return out, nil
	})
}
