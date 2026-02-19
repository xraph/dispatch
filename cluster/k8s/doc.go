// Package k8s provides a Kubernetes-native cluster.Store implementation.
//
// Worker discovery uses Pod annotations with a configurable label selector.
// Leader election uses the coordination/v1 Lease API (standard K8s pattern).
//
// Example:
//
//	client := kubernetes.NewForConfigOrDie(rest.InClusterConfig())
//	provider := k8s.New(client, "my-namespace")
//	// Use provider as a cluster.Store
package k8s
