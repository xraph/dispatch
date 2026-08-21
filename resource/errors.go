package resource

import "errors"

// ErrCapacityExceeded means an acquisition could not be satisfied: either
// the request is larger than total capacity and never can be, or every
// holder still held its share when the caller's context ended.
var ErrCapacityExceeded = errors.New("dispatch/resource: capacity exceeded")

// ErrUnschedulable means a requirement exceeds the largest known worker
// capacity, so no worker could ever run it. Returned at enqueue so the
// job fails on a developer's machine rather than pending forever.
var ErrUnschedulable = errors.New("dispatch/resource: no worker can satisfy the requirement")
