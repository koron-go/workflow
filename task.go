package workflow

import "context"

// Task represents a task of workflow.
type Task struct {
	Name     string
	Requires []*Task
	Runner   Runner
}

// Start starts a task with WorkflowContext.
func (task *Task) Start(wCtx *WorkflowContext) error {
	ctx, cancel := context.WithCancel(wCtx.ctx)
	defer cancel()
	taskCtx := &TaskContext{
		task:   task,
		wCtx:   wCtx,
		ctx:    ctx,
		cancel: cancel,
	}
	wCtx.putTaskContext(taskCtx)
	err := task.Runner.Run(taskCtx)
	if err != nil {
		return err
	}
	return nil
}

func (task *Task) isRequire(target *Task) bool {
	for _, req := range task.Requires {
		if target == req {
			return true
		}
	}
	return false
}

// TaskContext is a context for an executing task.
type TaskContext struct {
	task   *Task
	wCtx   *WorkflowContext
	ctx    context.Context
	cancel context.CancelFunc
}

// Context gets a context.Context of a Task.
func (taskCtx *TaskContext) Context() context.Context {
	return taskCtx.ctx
}

// WorkflowContext obtains workflow's context parent of this task.
func (taskCtx *TaskContext) WorkflowContext() *WorkflowContext {
	return taskCtx.wCtx
}

// Cancel sends cancel signal to a Task.
func (taskCtx *TaskContext) Cancel() {
	if taskCtx.cancel != nil {
		taskCtx.cancel()
	}
}

// SetOutput sets output data of a Task.
func (taskCtx *TaskContext) SetOutput(v interface{}) {
	taskCtx.wCtx.setOutput(taskCtx.task, v)
}

// Input gets output of required Task.
func (taskCtx *TaskContext) Input(task *Task) (interface{}, error) {
	if !taskCtx.task.isRequire(task) {
		return nil, ErrNotRequired
	}
	v := taskCtx.wCtx.output(task)
	if v == nil {
		return nil, ErrNoTaskOutput
	}
	return v, nil
}
