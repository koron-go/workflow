package workflow

import "context"

// Runner provides a procedure of Task.
type Runner interface {
	Run(context.Context) error
}

// RunnerFunc is function wrapper for Runner.
type RunnerFunc func(context.Context) error

// Run runs procedure of Task.
func (fn RunnerFunc) Run(ctx context.Context) error {
	return fn(ctx)
}

type nullRunner struct{}

func (nullRunner) Run(context.Context) error {
	return nil
}

// ExitHandler will be called when a workflow exit.  It can be registered by
// TaskContext.AtExit() function.
type ExitHandler func(ctx context.Context)
