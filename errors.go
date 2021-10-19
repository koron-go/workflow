package workflow

import (
	"errors"
	"fmt"
)

// ErrNoTasks happens when no tasks to be executed.
var ErrNoTasks = errors.New("no tasks")

// ErrNoOutput happens when task did't provide output.
var ErrNoOutput = errors.New("task did't provide output")

// ErrNotInWorkflow happens when a task is not in workflow.
var ErrNotInWorkflow = errors.New("task is not in workflow")

// ErrNoWorkflows happens when no workflows in context.
var ErrNoWorkflows = errors.New("no workflows")

// Error wraps errors happen in each tasks, and not executed tasks.
type Error struct {
	// Idle has tasks which not executed.
	Idle []*Task
	// Failed has errors for each tasks.
	Failed map[*Task]error
}

func (err *Error) Error() string {
	return fmt.Sprintf("workflow failed: %d tasks not run, %d tasks have error", len(err.Idle), len(err.Failed))
}
