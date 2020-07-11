package workflow

import "context"

// Context provides methods to access running/completed workflow.
type Context interface {
	// Wait waits all terminal tasks are terminated.
	Wait(ctx context.Context) error
}
