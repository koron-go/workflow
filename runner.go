package workflow

import "context"

// Runner provides a procedure of Task.
type Runner interface {
	Run(*TaskContext) error
}

// RunnerFunc is function wrapper for Runner.
type RunnerFunc func(*TaskContext) error

// Run runs procedure of Task.
func (fn RunnerFunc) Run(taskCtx *TaskContext) error {
	return fn(taskCtx)
}

type nullRunner struct{}

func (nullRunner) Run(*TaskContext) error {
	return nil
}

// ExitHandler will be called when a workflow exit.  It can be registered by
// TaskContext.AtExit() function.
type ExitHandler func(ctx context.Context)
