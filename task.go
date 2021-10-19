package workflow

import "context"

// Task represents a task of workflow.
type Task struct {
	name string

	runner Runner

	dep dependency
}

// NewTask creates a new Task object with name and runner.
// new Task will start when all "requires" tasks are successfully completed.
func NewTask(name string, runner Runner, requires ...*Task) *Task {
	return &Task{
		name:   name,
		runner: runner,
		dep:    make(dependency).when(onComplete, requires...),
	}
}

// Name returns name of task.
func (task *Task) Name() string {
	return task.name
}

// WithName changes name of task.
func (task *Task) WithName(name string) *Task {
	task.name = name
	return task
}

// WithRunner changes runner of task
func (task *Task) WithRunner(runner Runner) *Task {
	task.runner = runner
	return task
}

// WhenComplete adds depended tasks, this task will start when those tasks
// successfully completed.
func (task *Task) WhenComplete(tasks ...*Task) *Task {
	task.dep.when(onComplete, tasks...)
	return task
}

// WhenStart adds depended tasks, this task will start when those tasks
// started.
func (task *Task) WhenStart(tasks ...*Task) *Task {
	task.dep.when(onStart, tasks...)
	return task
}

// WhenEnd adds depended tasks, this task will start when those tasks ended.
func (task *Task) WhenEnd(tasks ...*Task) *Task {
	task.dep.when(onEnd, tasks...)
	return task
}

func (task *Task) getRunner() Runner {
	if task.runner == nil {
		return nullRunner{}
	}
	return task.runner
}

// taskContext is a context for an executing task.
type taskContext struct {
	wCtx *workflowContext

	name   string
	runner Runner
	dep    dependency

	cancel context.CancelFunc

	started bool
	ended   bool

	output interface{}
	err    error
}

type taskKey struct{}

var taskKey0 taskKey

func (taskCtx *taskContext) runTask(wCtx *workflowContext) {
	ctx0, cancel := context.WithCancel(wCtx.ctx)
	defer cancel()
	ctx := context.WithValue(ctx0, taskKey0, taskCtx)
	taskCtx.cancel = cancel
	taskCtx.wCtx.log.Printf("[workflow.task:%s] start", taskCtx.name)
	err := taskCtx.runner.Run(ctx)
	taskCtx.wCtx.log.Printf("[workflow.task:%s] end", taskCtx.name)
	wCtx.taskCompleted(taskCtx, err)
}

// getTaskContext extracts task context from binded context.Context
func getTaskContext(ctx context.Context) (*taskContext, bool) {
	taskCtx, ok := ctx.Value(taskKey0).(*taskContext)
	return taskCtx, ok
}

// mustGetTaskContext extracts task context from binded context.Context.
// If context.Context doesn't have context.Context this will panic.
func mustGetTaskContext(ctx context.Context) *taskContext {
	taskCtx, ok := getTaskContext(ctx)
	if !ok {
		panic("context.Context didn't bind to *workflow.TaskContext")
	}
	return taskCtx
}

// TaskName returns name of task which corresponding context.
func TaskName(ctx context.Context) string {
	taskCtx, ok := getTaskContext(ctx)
	if !ok {
		return "" // no task context binded
	}
	return taskCtx.name
}

// CancelTask cancels another tasks in a workflow.
// This can't cancel myself nor tasks not in a workflow.
func CancelTask(ctx context.Context, tasks ...*Task) {
	taskCtx := mustGetTaskContext(ctx)
	wCtx := mustGetWorkflowContext(ctx)
	for _, task := range tasks {
		otherCtx, ok := wCtx.contexts[task]
		if !ok || otherCtx == taskCtx {
			continue
		}
		otherCtx.cancel()
	}
}

// SetResult sets result value of a task which corresponding to context.
func SetResult(ctx context.Context, v interface{}) {
	wCtx := mustGetWorkflowContext(ctx)
	wCtx.rw.Lock()
	taskCtx := mustGetTaskContext(ctx)
	taskCtx.output = v
	wCtx.rw.Unlock()
}

// Result gets result value of completed task in a workflow.
func Result(ctx context.Context, task *Task) (interface{}, error) {
	wCtx, ok := getWorkflowContext(ctx)
	if !ok {
		return nil, ErrNoWorkflows
	}
	wCtx.rw.RLock()
	defer wCtx.rw.RUnlock()
	otherCtx, ok := wCtx.contexts[task]
	if !ok {
		return nil, ErrNotInWorkflow
	}
	if otherCtx.err != nil {
		return nil, otherCtx.err
	}
	if otherCtx.output == nil {
		return nil, ErrNoOutput
	}
	return otherCtx.output, nil
}
