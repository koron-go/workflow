package workflow

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
