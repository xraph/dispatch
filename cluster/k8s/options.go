package k8s

import "log/slog"

// Option configures a Provider.
type Option func(*Provider)

// WithLogger sets a custom logger.
func WithLogger(l *slog.Logger) Option {
	return func(p *Provider) { p.logger = l }
}

// WithLeaseName sets the Lease object name used for leader election.
// Default: "dispatch-leader".
func WithLeaseName(name string) Option {
	return func(p *Provider) { p.leaseName = name }
}

// WithLabelSelector overrides the label selector used to discover worker Pods.
// Default: "app.kubernetes.io/component=dispatch-worker".
func WithLabelSelector(sel string) Option {
	return func(p *Provider) { p.labelSelector = sel }
}

// WithAnnotationPrefix sets the prefix for worker-data annotations on Pods.
// Default: "dispatch.xraph.com/".
func WithAnnotationPrefix(prefix string) Option {
	return func(p *Provider) { p.annotationPrefix = prefix }
}
