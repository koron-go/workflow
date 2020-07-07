package workflow

import (
	"context"
	"sync"
)

// Run executes workflow componsed from tasks.
func Run(ctx context.Context, tasks ...*Task) error {
	if len(tasks) == 0 {
		return ErrNoTasks
	}
	wCtx := newWorkflowContext(ctx)
	defer wCtx.cancel()
	// TODO: enumerate and execute all tasks.
	wCtx.clear()
	return nil
}

// WorkflowContext is a context for an executing workflow.
type WorkflowContext struct {
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.RWMutex
	outputs  map[*Task]interface{}
	contexts map[*Task]*TaskContext
}

func newWorkflowContext(ctx context.Context) *WorkflowContext {
	ctx, cancel := context.WithCancel(ctx)
	return &WorkflowContext{
		ctx:      ctx,
		cancel:   cancel,
		outputs:  make(map[*Task]interface{}),
		contexts: make(map[*Task]*TaskContext),
	}
}

func (wCtx *WorkflowContext) clear() {
	wCtx.outputs = nil
	wCtx.contexts = nil
}

func (wCtx *WorkflowContext) setOutput(task *Task, v interface{}) {
	wCtx.mu.Lock()
	wCtx.outputs[task] = v
	wCtx.mu.Unlock()
}

func (wCtx *WorkflowContext) output(task *Task) interface{} {
	wCtx.mu.RLock()
	v := wCtx.outputs[task]
	wCtx.mu.RUnlock()
	return v
}

func (wCtx *WorkflowContext) putTaskContext(taskCtx *TaskContext) {
	wCtx.mu.Lock()
	wCtx.contexts[taskCtx.task] = taskCtx
	wCtx.mu.Unlock()
}

func (wCtx *WorkflowContext) taskContext(task *Task) *TaskContext {
	wCtx.mu.RLock()
	defer wCtx.mu.RUnlock()
	taskCtx, ok := wCtx.contexts[task]
	if !ok {
		return nil
	}
	return taskCtx
}

// Context gets a context.Context of a workflow.
func (wCtx *WorkflowContext) Context() context.Context {
	return wCtx.ctx
}

// Cancel sends cancel signal to a workflow.
func (wCtx *WorkflowContext) Cancel() {
	if wCtx.cancel != nil {
		wCtx.cancel()
	}
}

// TaskContext obtains a task context for a task.
func (wCtx *WorkflowContext) TaskContext(task *Task) *TaskContext {
	return wCtx.taskContext(task)
}
