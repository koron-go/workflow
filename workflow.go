package workflow

import (
	"context"
	"sync"
	"sync/atomic"
)

// workflowContext is a context for an executing workflow.
type workflowContext struct {
	ctx    context.Context
	cancel context.CancelFunc

	log Logger

	rw   sync.RWMutex
	quit chan struct{}

	contexts map[*Task]*TaskContext
	idling   map[*TaskContext]struct{}
	running  map[*TaskContext]struct{}

	atExit []ExitHandler

	waitMu sync.Mutex
	waitDo int32
	err    error
}

func (wCtx *workflowContext) prepareTaskContext(task *Task) *TaskContext {
	if taskCtx, ok := wCtx.contexts[task]; ok {
		return taskCtx
	}
	runner := task.runner
	if runner == nil {
		runner = nullRunner{}
	}
	taskCtx := &TaskContext{
		wCtx:   wCtx,
		name:   task.Name,
		runner: task.runner,
		dep:    task.dep.clone(),
	}
	wCtx.contexts[task] = taskCtx
	wCtx.idling[taskCtx] = struct{}{}
	task.dep.walk(func(dependedTask *Task) {
		wCtx.prepareTaskContext(dependedTask)
	})
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
	for {
		started := 0
		for taskCtx := range wCtx.idling {
			if !taskCtx.dep.isSatisfy(wCtx) {
				continue
			}
			delete(wCtx.idling, taskCtx)
			wCtx.running[taskCtx] = struct{}{}
			go taskCtx.runTask(wCtx)
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

// Wait waits all tasks are terminated.
func (wCtx *workflowContext) Wait(ctx context.Context) error {
	if atomic.LoadInt32(&wCtx.waitDo) == 1 {
		return wCtx.err
	}
	wCtx.waitMu.Lock()
	defer wCtx.waitMu.Unlock()
	if wCtx.waitDo == 1 {
		return wCtx.err
	}
	// wait all tasks are stopped.
	<-wCtx.quit
	// run all exit runners
	for i := len(wCtx.atExit) - 1; i >= 0; i-- {
		if h := wCtx.atExit[i]; h != nil {
			h(ctx)
		}
	}
	wCtx.err = wCtx.finish()
	atomic.StoreInt32(&wCtx.waitDo, 1)
	wCtx.cancel()
	return wCtx.err
}

// Run executes a workflow with termination tasks.  All tasks which depended by
// termination tasks and recursively dependeds will be executed.
func Run(ctx context.Context, tasks ...*Task) error {
	w := New(tasks...)
	c, err := w.Start(ctx)
	if err != nil {
		return err
	}
	return c.Wait(context.Background())
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

// Start starts a workflow.
func (w *Workflow) Start(ctx context.Context) (Context, error) {
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

func (w *Workflow) newContext(ctx context.Context) *workflowContext {
	ctx, cancel := context.WithCancel(ctx)
	log := w.log
	if log == nil {
		log = discardLogger{}
	}
	return &workflowContext{
		ctx:      ctx,
		cancel:   cancel,
		log:      log,
		quit:     make(chan struct{}),
		contexts: make(map[*Task]*TaskContext),
		idling:   make(map[*TaskContext]struct{}),
		running:  make(map[*TaskContext]struct{}),
	}
}
