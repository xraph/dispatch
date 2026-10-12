package dashboard

import (
	"github.com/xraph/forge/extensions/dashboard/contributor"

	"github.com/xraph/dispatch/dashboard/components"
)

// NewManifest builds a contributor.Manifest for the dispatch dashboard.
func NewManifest() *contributor.Manifest {
	return &contributor.Manifest{
		Name:        "dispatch",
		DisplayName: "Dispatch",
		Icon:        "zap",
		Version:     "0.1.0",
		Layout:      "extension",
		ShowSidebar: boolPtr(true),
		TopbarConfig: &contributor.TopbarConfig{
			Title:       "Dispatch",
			LogoIcon:    "zap",
			AccentColor: "#f59e0b",
			ShowSearch:  true,
			Actions: []contributor.TopbarAction{
				{Label: "API Docs", Icon: "file-text", Href: "/docs", Variant: "ghost"},
			},
		},
		SidebarFooterContent: components.FooterAPIDocsLink("/docs"),
		Nav:                  baseNav(),
		Widgets:              baseWidgets(),
		Settings:             baseSettings(),
	}
}

func baseNav() []contributor.NavItem {
	return []contributor.NavItem{
		{Label: "Overview", Path: "/", Icon: "layout-dashboard", Group: "Dispatch", Priority: 0},
		{Label: "Jobs", Path: "/jobs", Icon: "briefcase", Group: "Operations", Priority: 1},
		{Label: "Workflows", Path: "/workflows", Icon: "git-branch", Group: "Operations", Priority: 2},
		{Label: "Dead Letters", Path: "/dlq", Icon: "alert-triangle", Group: "Operations", Priority: 3},
		{Label: "Queues", Path: "/queues", Icon: "layers", Group: "Monitoring", Priority: 4},
		{Label: "Workers", Path: "/workers", Icon: "server", Group: "Monitoring", Priority: 5},
		{Label: "Cron", Path: "/crons", Icon: "clock", Group: "Scheduling", Priority: 6},
		{Label: "Handlers", Path: "/handlers", Icon: "package", Group: "Configuration", Priority: 7},
	}
}

func baseWidgets() []contributor.WidgetDescriptor {
	return []contributor.WidgetDescriptor{
		{
			ID:          "dispatch-stats",
			Title:       "Job Stats",
			Description: "Job counts by state",
			Size:        "md",
			RefreshSec:  30,
			Group:       "Dispatch",
		},
		{
			ID:          "dispatch-recent-jobs",
			Title:       "Recent Jobs",
			Description: "Latest job activity",
			Size:        "lg",
			RefreshSec:  15,
			Group:       "Dispatch",
		},
		{
			ID:          "dispatch-dlq",
			Title:       "Dead Letters",
			Description: "DLQ and failed job counts",
			Size:        "sm",
			RefreshSec:  30,
			Group:       "Dispatch",
		},
		{
			ID:          "dispatch-queue-activity",
			Title:       "Queue Activity",
			Description: "Per-queue active job counts",
			Size:        "md",
			RefreshSec:  15,
			Group:       "Dispatch",
		},
		{
			ID:          "dispatch-cluster-health",
			Title:       "Cluster Health",
			Description: "Worker instances and cluster status",
			Size:        "md",
			RefreshSec:  15,
			Group:       "Dispatch",
		},
		{
			ID:          "dispatch-cron-schedule",
			Title:       "Cron Schedules",
			Description: "Upcoming scheduled job executions",
			Size:        "md",
			RefreshSec:  60,
			Group:       "Dispatch",
		},
	}
}

func baseSettings() []contributor.SettingsDescriptor {
	return []contributor.SettingsDescriptor{
		{
			ID:          "dispatch-config",
			Title:       "Engine Settings",
			Description: "Dispatch engine configuration",
			Group:       "Dispatch",
			Icon:        "zap",
		},
	}
}

func boolPtr(b bool) *bool { return &b }
