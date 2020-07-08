package workflow

import "context"

// Task represents a task of workflow.
type Task struct {
	Name   string

	runner Runner

	requires []*Task
}

// NewTask creates a new Task object with name and runner.
func NewTask(name string, runner Runner, requires ...*Task) *Task {
	return &Task{
		Name:     name,
		Runner:   runner,
		requires: requires,
	}
}

// AddRequires adds required tasks to start this taks.
func (task *Task) AddRequires(targets []*Task) *Task {
	task.requires = append(task.requires, targets...)
	return task
}

func (task *Task) isRequire(target *Task) bool {
	for _, req := range task.requires {
		if target == req {
			return true
		}
	}
	return false
}

// TaskContext is a context for an executing task.
type TaskContext struct {
	task *Task
	wCtx *Context

	ctx    context.Context
	cancel context.CancelFunc

	output interface{}
	ended  bool
	err    error
}

// Context gets a context.Context of a Task.
func (taskCtx *TaskContext) Context() context.Context {
	return taskCtx.ctx
}

// WorkflowContext obtains workflow's context, parent of this task.
func (taskCtx *TaskContext) WorkflowContext() *Context {
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
	taskCtx.wCtx.rw.Lock()
	taskCtx.output = v
	taskCtx.wCtx.rw.Unlock()
}

// Input gets output of required Task.
func (taskCtx *TaskContext) Input(task *Task) (interface{}, error) {
	if !taskCtx.task.isRequire(task) {
		return nil, ErrNotRequired
	}
	return taskCtx.wCtx.getTaskOutput(task)
}

func (taskCtx *TaskContext) canStart(wCtx *Context) bool {
	for _, req := range taskCtx.task.requires {
		reqCtx := wCtx.taskContext(req)
		if reqCtx != nil && !reqCtx.ended {
			return false
		}
	}
	return true
}

func (taskCtx *TaskContext) start(wCtx *Context) {
	taskCtx.ctx, taskCtx.cancel = context.WithCancel(wCtx.ctx)
	defer taskCtx.cancel()
	if r := taskCtx.task.Runner; r != nil {
		err := r.Run(taskCtx)
		if err != nil {
			wCtx.taskCompleted(taskCtx, err)
			return
		}
	}
	wCtx.taskCompleted(taskCtx, nil)
}
