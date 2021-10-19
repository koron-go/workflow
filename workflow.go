package workflow

import (
	"context"
	"sync"
)

// workflowContext is a context for an executing workflow.
type workflowContext struct {
	ctx    context.Context
	cancel context.CancelFunc

	log Logger

	rw   sync.RWMutex
	quit chan struct{}

	contexts map[*Task]*taskContext
	idling   map[*taskContext]struct{}
	running  map[*taskContext]struct{}

	atExit []ExitHandler
}

func (wCtx *workflowContext) prepareTaskContext(task *Task) *taskContext {
	if taskCtx, ok := wCtx.contexts[task]; ok {
		return taskCtx
	}
	taskCtx := &taskContext{
		wCtx:   wCtx,
		name:   task.name,
		runner: task.getRunner(),
		dep:    task.dep.clone(),
	}
	wCtx.contexts[task] = taskCtx
	wCtx.idling[taskCtx] = struct{}{}
	task.dep.walk(func(dependedTask *Task) {
		wCtx.prepareTaskContext(dependedTask)
	})
	return taskCtx
}

func (wCtx *workflowContext) taskCompleted(taskCtx *taskContext, err error) {
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
	for {
		started := 0
		for taskCtx := range wCtx.idling {
			if !taskCtx.dep.isSatisfy(wCtx) {
				continue
			}
			delete(wCtx.idling, taskCtx)
			wCtx.running[taskCtx] = struct{}{}
			go taskCtx.runTask(wCtx.ctx)
			taskCtx.started = true
			started++
		}
		if started == 0 {
			break
		}
	}
	if len(wCtx.running) > 0 {
		return
	}
	// finish a workflow
	close(wCtx.quit)
}

func (wCtx *workflowContext) finish() error {
	defer wCtx.cancel()
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

// wait waits all tasks are terminated.
func (wCtx *workflowContext) wait(ctx context.Context) error {
	// wait all tasks are stopped.
	<-wCtx.quit
	// run all exit runners
	for i := len(wCtx.atExit) - 1; i >= 0; i-- {
		if h := wCtx.atExit[i]; h != nil {
			h(ctx)
		}
	}
	return wCtx.finish()
}

// Run executes a workflow with termination tasks.  All tasks which depended by
// termination tasks and recursively depended tasks will be executed.
func Run(ctx context.Context, tasks ...*Task) error {
	return New(tasks...).Run(ctx)
}

// Workflow represents a workflow definition.
type Workflow struct {
	tasks []*Task
	log   Logger
}

// New create a workflow definition with terminal tasks.
func New(tasks ...*Task) *Workflow {
	return &Workflow{
		tasks: tasks,
	}
}

// Run executes a workflow with its definition.  All tasks which depended by
// termination tasks and recursively depended tasks will be executed.
func (w *Workflow) Run(ctx context.Context) error {
	c, err := w.start(ctx)
	if err != nil {
		return err
	}
	return c.wait(context.Background())
}

// SetLogger sets logger which log task's start/end logs.
// *log.Logger can be used for logger.
func (w *Workflow) SetLogger(log Logger) *Workflow {
	if log == nil {
		log = discardLogger{}
	}
	w.log = log
	return w
}

// Add adds terminal tasks.
func (w *Workflow) Add(tasks ...*Task) *Workflow {
	w.tasks = append(w.tasks, tasks...)
	return w
}

// start starts a workflow.
func (w *Workflow) start(ctx context.Context) (*workflowContext, error) {
	if len(w.tasks) == 0 {
		return nil, ErrNoTasks
	}
	wCtx := w.newContext(ctx)
	for _, task := range w.tasks {
		wCtx.prepareTaskContext(task)
	}
	wCtx.startTasks()
	return wCtx, nil
}

type workflowKey struct{}

var workflowKey0 workflowKey

func (w *Workflow) newContext(ctx context.Context) *workflowContext {
	ctx, cancel := context.WithCancel(ctx)
	log := w.log
	if log == nil {
		log = discardLogger{}
	}
	wCtx := &workflowContext{
		cancel:   cancel,
		log:      log,
		quit:     make(chan struct{}),
		contexts: make(map[*Task]*taskContext),
		idling:   make(map[*taskContext]struct{}),
		running:  make(map[*taskContext]struct{}),
	}
	wCtx.ctx = context.WithValue(ctx, workflowKey0, wCtx)
	return wCtx
}

func getWorkflowContext(ctx context.Context) (*workflowContext, bool) {
	wCtx, ok := ctx.Value(workflowKey0).(*workflowContext)
	return wCtx, ok
}

func mustGetWorkflowContext(ctx context.Context) *workflowContext {
	wCtx, ok := getWorkflowContext(ctx)
	if !ok {
		panic("context.Context didn't bind to workflow context")
	}
	return wCtx
}

// AtExit adds a handler, it will be called when a workflow exit.
func AtExit(ctx context.Context, h ExitHandler) {
	wCtx := mustGetWorkflowContext(ctx)
	wCtx.rw.Lock()
	wCtx.atExit = append(wCtx.atExit, h)
	wCtx.rw.Unlock()
}

// Cancel cancels a workflow which corresponding context.
func Cancel(ctx context.Context) {
	wCtx := mustGetWorkflowContext(ctx)
	wCtx.cancel()
}

// CancelTask cancels another tasks in a workflow.
// This can't cancel myself nor tasks not in a workflow.
func CancelTask(ctx context.Context, tasks ...*Task) {
	wCtx := mustGetWorkflowContext(ctx)
	taskCtx, _ := getTaskContext(ctx)
	for _, task := range tasks {
		otherCtx, ok := wCtx.contexts[task]
		if !ok || (taskCtx != nil && otherCtx == taskCtx) {
			continue
		}
		otherCtx.cancel()
	}
}
