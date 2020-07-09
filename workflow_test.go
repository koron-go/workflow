package workflow_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/koron-go/workflow"
)

func TestSingle(t *testing.T) {
	var sum int
	task := workflow.NewTask(t.Name()+"_1",
		workflow.RunnerFunc(func(*workflow.TaskContext) error {
			sum += 2
			return nil
		}),
	)
	err := workflow.Run(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}
	if sum != 2 {
		t.Fatalf("unexpected sum: want=2 got=%d", sum)
	}
}

func TestTaskName(t *testing.T) {
	var sum int
	name := t.Name() + "_1"
	task := workflow.NewTask(name,
		workflow.RunnerFunc(func(taskCtx *workflow.TaskContext) error {
			sum += 2
			if s := taskCtx.Name(); s != name {
				t.Errorf("unexpected name: want=%s got=%s", name, s)
			}
			return nil
		}),
	)
	err := workflow.Run(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}
	if sum != 2 {
		t.Fatalf("unexpected sum: want=2 got=%d", sum)
	}
}

func TestNoTaks(t *testing.T) {
	err := workflow.Run(context.Background())
	if err == nil {
		t.Fatal("workflow should be failed")
	}
	if !errors.Is(err, workflow.ErrNoTasks) {
		t.Fatalf("unexpected error: want=%s got=%s", workflow.ErrNoTasks, err)
	}
}

func TestSequential(t *testing.T) {
	sum := 2
	task1 := workflow.NewTask(t.Name()+"_1",
		workflow.RunnerFunc(func(*workflow.TaskContext) error {
			sum *= 3
			return nil
		}),
	)
	task2 := workflow.NewTask(t.Name()+"_2",
		workflow.RunnerFunc(func(*workflow.TaskContext) error {
			sum += 5
			return nil
		}), task1,
	)
	task3 := workflow.NewTask(t.Name()+"_3",
		workflow.RunnerFunc(func(*workflow.TaskContext) error {
			sum *= 7
			return nil
		}), task2,
	)
	err := workflow.Run(context.Background(), task3)
	if err != nil {
		t.Fatal(err)
	}
	exp := (2*3 + 5) * 7
	if sum != exp {
		t.Fatalf("unexpected sum: want=%d got=%d", exp, sum)
	}
}

func TestParallel(t *testing.T) {
	sum := 2
	task1 := workflow.NewTask(t.Name()+"_1",
		workflow.RunnerFunc(func(*workflow.TaskContext) error {
			sum *= 3
			return nil
		}),
	)
	task2 := workflow.NewTask(t.Name()+"_2",
		workflow.RunnerFunc(func(*workflow.TaskContext) error {
			sum += 5
			return nil
		}), task1,
	)
	task3 := workflow.NewTask(t.Name()+"_3",
		workflow.RunnerFunc(func(*workflow.TaskContext) error {
			sum += 7
			return nil
		}), task1,
	)
	task4 := workflow.NewTask(t.Name()+"_4",
		workflow.RunnerFunc(func(*workflow.TaskContext) error {
			sum *= 11
			return nil
		}), task2, task3,
	)
	err := workflow.Run(context.Background(), task4)
	if err != nil {
		t.Fatal(err)
	}
	exp := ((2 * 3) + 5 + 7) * 11
	if sum != exp {
		t.Fatalf("unexpected sum: want=%d got=%d", exp, sum)
	}
}

func TestCancelWorkflow(t *testing.T) {
	sum := 2
	task1 := workflow.NewTask(t.Name()+"_1",
		workflow.RunnerFunc(func(taskCtx *workflow.TaskContext) error {
			ctx := taskCtx.Context()
			sum *= 3
			<-ctx.Done()
			err := ctx.Err()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("error is not DeadlineExceeded: %s", err)
			}
			sum += 5
			return nil
		}),
	)
	st := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := workflow.Run(ctx, task1)
	d := time.Since(st)
	if err != nil {
		t.Fatal(err)
	}
	if d < 10*time.Millisecond {
		t.Fatal("timeout not works")
	}
	exp := (2 * 3) + 5
	if sum != exp {
		t.Fatalf("unexpected sum: want=%d got=%d", exp, sum)
	}
}

func TestCancelTask(t *testing.T) {
	sum := 2
	task1 := workflow.NewTask(t.Name()+"_1",
		workflow.RunnerFunc(func(taskCtx *workflow.TaskContext) error {
			sum *= 3
			return nil
		}),
	)
	task2 := workflow.NewTask(t.Name()+"_2",
		workflow.RunnerFunc(func(taskCtx *workflow.TaskContext) error {
			sum += 5
			st := time.Now()
			ctx := taskCtx.Context()
			<-ctx.Done()
			d := time.Since(st)
			err := ctx.Err()
			if !errors.Is(err, context.Canceled) {
				t.Errorf("error is not  Canceled: %s", err)
			}
			if d < 10*time.Millisecond {
				t.Errorf("too short delay, something wrong: %s", d)
			}
			sum *= 7
			return nil
		}), task1,
	)
	task3 := workflow.NewTask(t.Name()+"_3",
		workflow.RunnerFunc(func(taskCtx *workflow.TaskContext) error {
			sum += 11
			time.Sleep(15 * time.Millisecond)
			return nil
		}), task1,
	)
	task4 := workflow.NewTask(t.Name()+"_4",
		workflow.RunnerFunc(func(taskCtx *workflow.TaskContext) error {
			sum *= 13
			taskCtx.CancelTask(task2)
			return nil
		}), task3,
	)
	err := workflow.Run(context.Background(), task2, task4)
	if err != nil {
		t.Fatal(err)
	}
	exp := ((2 * 3) + 5 + 11) * 13 * 7
	if sum != exp {
		t.Fatalf("unexpected sum: want=%d got=%d", exp, sum)
	}
}

func TestError(t *testing.T) {
	task1 := workflow.NewTask(t.Name()+"_1",
		workflow.RunnerFunc(func(*workflow.TaskContext) error {
			return errors.New("planned error")
		}),
	)
	task2 := workflow.NewTask(t.Name()+"_2",
		workflow.RunnerFunc(func(*workflow.TaskContext) error {
			t.Error("never executed")
			return nil
		}), task1,
	)
	err := workflow.Run(context.Background(), task2)
	if err == nil {
		t.Fatal("workflow should be failed")
	}
	exp := `workflow failed: 1 tasks not run, 1 tasks have error`
	if s := err.Error(); s != exp {
		t.Fatalf("unexpected error:\nwant=%s\ngot=%s", exp, s)
	}
}

func TestOutputInput(t *testing.T) {
	sum := 2
	task1 := workflow.NewTask(t.Name()+"_1",
		workflow.RunnerFunc(func(taskCtx *workflow.TaskContext) error {
			sum *= 3
			taskCtx.SetOutput(sum)
			return nil
		}),
	)
	task2 := workflow.NewTask(t.Name()+"_2",
		workflow.RunnerFunc(func(taskCtx *workflow.TaskContext) error {
			sum += 5
			taskCtx.SetOutput(sum)
			return nil
		}), task1,
	)
	task3 := workflow.NewTask(t.Name()+"_3",
		workflow.RunnerFunc(func(taskCtx *workflow.TaskContext) error {
			o1, err := taskCtx.Input(task1)
			if err != nil {
				return err
			}
			o2, err := taskCtx.Input(task2)
			if err != nil {
				return err
			}
			sum = o1.(int) * o2.(int) * 7
			return nil
		}), task2,
	)
	err := workflow.Run(context.Background(), task3)
	if err != nil {
		t.Fatal(err)
	}
	exp := (2 * 3) * (2*3 + 5) * 7
	if sum != exp {
		t.Fatalf("unexpected sum: want=%d got=%d", exp, sum)
	}
}
