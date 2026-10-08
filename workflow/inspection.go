package workflow

import "sort"

// Versions returns the registered versions in ascending order.
func (r *Registry) Versions(name string) []int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	versions := make([]int, 0, len(r.versions[name]))
	for _, entry := range r.versions[name] {
		versions = append(versions, entry.version)
	}
	sort.Ints(versions)
	return versions
}
