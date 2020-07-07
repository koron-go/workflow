package workflow

import "errors"

// ErrNoTasks happens when no tasks to be executed.
var ErrNoTasks = errors.New("no tasks")

// ErrNotRequired happens when try to obtain output for Task which not required
// by current Task.
var ErrNotRequired = errors.New("task is not required")

// ErrNoTaskOutput happens when task did't provide output.
var ErrNoTaskOutput = errors.New("task did't provide output")
