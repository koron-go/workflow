package workflow

import (
	"context"
	"sync"
)

// Context is a context for an executing workflow.
type Context struct {
	ctx    context.Context
	cancel context.CancelFunc

	rw   sync.RWMutex
	quit chan struct{}

	contexts map[*Task]*TaskContext
	idling   map[*TaskContext]struct{}
	running  map[*TaskContext]struct{}
}

func newWorkflowContext(ctx context.Context) *Context {
	ctx, cancel := context.WithCancel(ctx)
	return &Context{
		ctx:      ctx,
		cancel:   cancel,
		quit:     make(chan struct{}),
		contexts: make(map[*Task]*TaskContext),
		idling:   make(map[*TaskContext]struct{}),
		running:  make(map[*TaskContext]struct{}),
	}
}

func (wCtx *Context) getTaskOutput(task *Task) (interface{}, error) {
	wCtx.rw.RLock()
	defer wCtx.rw.RUnlock()
	taskCtx, ok := wCtx.contexts[task]
	if !ok {
		return nil, ErrNotInWorkflow
	}
	if taskCtx.err != nil {
		return nil, taskCtx.err
	}
	if taskCtx.output == nil {
		return nil, ErrNoOutput
	}
	return taskCtx.output, nil
}

func (wCtx *Context) taskContext(task *Task) *TaskContext {
	taskCtx, ok := wCtx.contexts[task]
	if !ok {
		return nil
	}
	return taskCtx
}

// Context gets a context.Context of a workflow.
func (wCtx *Context) Context() context.Context {
	return wCtx.ctx
}

// Cancel sends cancel signal to a workflow.
func (wCtx *Context) Cancel() {
	if wCtx.cancel != nil {
		wCtx.cancel()
	}
}

// TaskContext obtains a task context for a task.
func (wCtx *Context) TaskContext(task *Task) *TaskContext {
	return wCtx.taskContext(task)
}

func (wCtx *Context) prepareTaskContext(task *Task) *TaskContext {
	if taskContext, ok := wCtx.contexts[task]; ok {
		return taskContext
	}
	taskCtx := &TaskContext{
		task: task,
		wCtx: wCtx,
	}
	wCtx.contexts[task] = taskCtx
	wCtx.idling[taskCtx] = struct{}{}
	for _, requireTask := range task.requires {
		wCtx.prepareTaskContext(requireTask)
	}
	return taskCtx
}

func (wCtx *Context) taskCompleted(taskCtx *TaskContext, err error) {
	wCtx.rw.Lock()
	taskCtx.err = err
	delete(wCtx.running, taskCtx)
	wCtx.rw.Unlock()
	wCtx.startTasks()
}

func (wCtx *Context) startTasks() {
	wCtx.rw.Lock()
	defer wCtx.rw.Unlock()
	for taskCtx := range wCtx.idling {
		if !taskCtx.canStart(wCtx) {
			continue
		}
		delete(wCtx.idling, taskCtx)
		wCtx.running[taskCtx] = struct{}{}
		go taskCtx.start(wCtx)
	}
	if len(wCtx.running) > 0 {
		return
	}
	// finish a workflow
	close(wCtx.quit)
}

func (wCtx *Context) finish() error {
	err := &Error{
		Failed: make(map[*Task]error),
	}
	for _, taskCtx := range wCtx.contexts {
		if taskCtx.err != nil {
			err.Failed[taskCtx.task] = taskCtx.err
		}
	}
	for taskCtx := range wCtx.idling {
		err.Idle = append(err.Idle, taskCtx.task)
	}
	wCtx.contexts = nil
	wCtx.idling = nil
	wCtx.running = nil
	if len(err.Failed) > 0 || len(err.Idle) > 0 {
		return err
	}
	return nil
}

// Run executes a workflow composed from tasks.
func Run(ctx context.Context, tasks ...*Task) error {
	if len(tasks) == 0 {
		return ErrNoTasks
	}
	wCtx := newWorkflowContext(ctx)
	defer wCtx.cancel()
	for _, task := range tasks {
		wCtx.prepareTaskContext(task)
	}
	wCtx.startTasks()
	// wait all tasks are stopped.
	<-wCtx.quit
	return wCtx.finish()
}