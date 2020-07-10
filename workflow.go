package workflow

import (
	"context"
	"sync"
)

// workflowContext is a context for an executing workflow.
type workflowContext struct {
	ctx    context.Context
	cancel context.CancelFunc

	rw   sync.RWMutex
	quit chan struct{}

	contexts map[*Task]*TaskContext
	idling   map[*TaskContext]struct{}
	running  map[*TaskContext]struct{}

	atExit []ExitHandler
}

func newWorkflowContext(ctx context.Context) *workflowContext {
	ctx, cancel := context.WithCancel(ctx)
	return &workflowContext{
		ctx:      ctx,
		cancel:   cancel,
		quit:     make(chan struct{}),
		contexts: make(map[*Task]*TaskContext),
		idling:   make(map[*TaskContext]struct{}),
		running:  make(map[*TaskContext]struct{}),
	}
}

func (wCtx *workflowContext) prepareTaskContext(task *Task) *TaskContext {
	if taskCtx, ok := wCtx.contexts[task]; ok {
		return taskCtx
	}
	taskCtx := &TaskContext{
		wCtx:     wCtx,
		name:     task.Name,
		runner:   task.runner,
		requires: task.copyRequires(),
	}
	wCtx.contexts[task] = taskCtx
	wCtx.idling[taskCtx] = struct{}{}
	for _, requireTask := range task.requires {
		wCtx.prepareTaskContext(requireTask)
	}
	return taskCtx
}

func (wCtx *workflowContext) taskCompleted(taskCtx *TaskContext, err error) {
	wCtx.rw.Lock()
	taskCtx.err = err
	taskCtx.ended = true
	delete(wCtx.running, taskCtx)
	wCtx.rw.Unlock()
	wCtx.startTasks()
}

func (wCtx *workflowContext) startTasks() {
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

func (wCtx *workflowContext) finish() error {
	err := &Error{
		Failed: make(map[*Task]error),
	}
	for task, taskCtx := range wCtx.contexts {
		if _, ok := wCtx.idling[taskCtx]; ok {
			err.Idle = append(err.Idle, task)
		}
		if taskCtx.err != nil {
			err.Failed[task] = taskCtx.err
		}
	}
	wCtx.contexts = nil
	wCtx.idling = nil
	wCtx.running = nil
	wCtx.atExit = nil
	if len(err.Failed) > 0 || len(err.Idle) > 0 {
		return err
	}
	return nil
}

// ExitHandler will be called when a workflow exit.  It can be registered by
// TaskContext.AtExit() function.
type ExitHandler func(ctx context.Context)

// Run executes a workflow with termination tasks.  All tasks which depended by
// termination tasks and recursively dependeds will be executed.
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
	// run all exit runners
	exitCtx := context.Background()
	for i := len(wCtx.atExit) - 1; i >= 0; i-- {
		if h := wCtx.atExit[i]; h != nil {
			h(exitCtx)
		}
	}
	return wCtx.finish()
}
