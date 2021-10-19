package workflow_test

import (
	"context"
	"testing"

	"github.com/koron-go/workflow"
)

func TestTask_Name(t *testing.T) {
	task := workflow.NewTask("foo", nil)
	if s := task.Name(); s != "foo" {
		t.Fatalf("unexpected task name: want=%s got=%s", "foo", s)
	}
	task.WithName("bar")
	if s := task.Name(); s != "bar" {
		t.Fatalf("unexpected task name: want=%s got=%s", "bar", s)
	}
}

func TestTask_WithRunner(t *testing.T) {
	sum := 2
	task := workflow.NewTask(t.Name()+"_1",
		workflow.RunnerFunc(func(context.Context) error {
			sum *= 3
			return nil
		}),
	)
	runTask := func(exp int) {
		t.Helper()
		err := workflow.Run(context.Background(), task)
		if err != nil {
			t.Fatal(err)
		}
		if sum != exp {
			t.Fatalf("unexpected sum: want=%d got=%d", exp, sum)
		}
	}
	runTask(2 * 3)
	// update runner of task
	task.WithRunner(workflow.RunnerFunc(func(context.Context) error {
		sum += 5
		return nil
	}))
	runTask(2*3 + 5)
}

func TestMustGetTaskContextPanic(t *testing.T) {
	defer func() {
		v := recover()
		if s, ok := v.(string); ok && s == "context.Context didn't bind to *workflow.TaskContext" {
			return
		}
		t.Fatalf("unexpected result, recover() returns: %v", v)
	}()
	_ = workflow.MustGetTaskContext(context.Background())
}
