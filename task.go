package workflow

import "context"

// Task represents a task of workflow.
type Task struct {
	Name string

	runner Runner

	requires []*Task
}

// NewTask creates a new Task object with name and runner.
func NewTask(name string, runner Runner, requires ...*Task) *Task {
	return &Task{
		Name:     name,
		runner:   runner,
		requires: requires,
	}
}

// Not need yet.
//// AddRequires adds required tasks to start this taks.
//func (task *Task) AddRequires(targets []*Task) *Task {
//	task.requires = append(task.requires, targets...)
//	return task
//}

func (task *Task) copyRequires() []*Task {
	if len(task.requires) == 0 {
		return nil
	}
	dst := make([]*Task, len(task.requires))
	copy(dst, task.requires)
	return dst
}

// TaskContext is a context for an executing task.
type TaskContext struct {
	wCtx *Context

	name     string
	runner   Runner
	requires []*Task

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

// CancelTask cancels another task in a workflow.
// This can't cancel myself or tasks not in a workflow.
func (taskCtx *TaskContext) CancelTask(task *Task) {
	otherCtx, ok := taskCtx.wCtx.contexts[task]
	if !ok || otherCtx == taskCtx {
		return
	}
	otherCtx.cancel()
}

// CancelWorkflow cancels a workflow, which current task belongs.
func (taskCtx *TaskContext) CancelWorkflow() {
	taskCtx.wCtx.cancel()
}

// SetOutput sets output data of a Task.
func (taskCtx *TaskContext) SetOutput(v interface{}) {
	taskCtx.wCtx.rw.Lock()
	taskCtx.output = v
	taskCtx.wCtx.rw.Unlock()
}

// Input gets output of required Task.
func (taskCtx *TaskContext) Input(task *Task) (interface{}, error) {
	return taskCtx.wCtx.getTaskOutput(task)
}

func (taskCtx *TaskContext) canStart(wCtx *Context) bool {
	for _, req := range taskCtx.requires {
		reqCtx, ok := wCtx.contexts[req]
		if ok && (!reqCtx.ended || reqCtx.err != nil) {
			return false
		}
	}
	return true
}

func (taskCtx *TaskContext) start(wCtx *Context) {
	taskCtx.ctx, taskCtx.cancel = context.WithCancel(wCtx.ctx)
	defer taskCtx.cancel()
	if r := taskCtx.runner; r != nil {
		err := r.Run(taskCtx)
		if err != nil {
			wCtx.taskCompleted(taskCtx, err)
			return
		}
	}
	wCtx.taskCompleted(taskCtx, nil)
}

func (taskCtx *TaskContext) Name() string {
	return taskCtx.name
}
