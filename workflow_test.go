package workflow_test

import (
	"bytes"
	"context"
	"errors"
	"log"
	"reflect"
	"testing"
	"time"

	"github.com/koron-go/workflow"
)

func TestSingle(t *testing.T) {
	var sum int
	task := workflow.NewTask(t.Name()+"_1",
		workflow.RunnerFunc(func(context.Context) error {
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
		workflow.RunnerFunc(func(ctx context.Context) error {
			sum += 2
			if s := workflow.TaskName(ctx); s != name {
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
		workflow.RunnerFunc(func(context.Context) error {
			sum *= 3
			return nil
		}),
	)
	task2 := workflow.NewTask(t.Name()+"_2",
		workflow.RunnerFunc(func(context.Context) error {
			sum += 5
			return nil
		}), task1,
	)
	task3 := workflow.NewTask(t.Name()+"_3",
		workflow.RunnerFunc(func(context.Context) error {
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
		workflow.RunnerFunc(func(context.Context) error {
			sum *= 3
			return nil
		}),
	)
	task2 := workflow.NewTask(t.Name()+"_2",
		workflow.RunnerFunc(func(context.Context) error {
			sum += 5
			return nil
		}), task1,
	)
	task3 := workflow.NewTask(t.Name()+"_3",
		workflow.RunnerFunc(func(context.Context) error {
			sum += 7
			return nil
		}), task1,
	)
	task4 := workflow.NewTask(t.Name()+"_4",
		workflow.RunnerFunc(func(context.Context) error {
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

func TestCancel(t *testing.T) {
	sum := 2
	task1 := workflow.NewTask(t.Name()+"_1",
		workflow.RunnerFunc(func(ctx context.Context) error {
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

func TestCancelWorkflow(t *testing.T) {
	sum := 2
	task1 := workflow.NewTask(t.Name()+"_1",
		workflow.RunnerFunc(func(ctx context.Context) error {
			<-ctx.Done()
			sum += 3
			return ctx.Err()
		}),
	)
	task2 := workflow.NewTask(t.Name()+"_2",
		workflow.RunnerFunc(func(ctx context.Context) error {
			<-ctx.Done()
			sum += 5
			return ctx.Err()
		}),
	)
	task3 := workflow.NewTask(t.Name()+"_3",
		workflow.RunnerFunc(func(ctx context.Context) error {
			sum *= 7
			time.Sleep(10 * time.Millisecond)
			workflow.Cancel(ctx)
			sum += 11
			return nil
		}),
	)
	task4 := workflow.NewTask(t.Name()+"_4",
		workflow.RunnerFunc(func(context.Context) error {
			sum *= 12
			return nil
		}), task1, task2, task3,
	)
	err := workflow.Run(context.Background(), task4)
	if err == nil {
		t.Fatal("workflow should be failed")
	}

	// check error details.
	expErr := `workflow failed: 1 tasks not run, 2 tasks have error`
	if s := err.Error(); s != expErr {
		t.Fatalf("unexpected error:\nwant=%s\ngot=%s", expErr, s)
	}
	if !reflect.DeepEqual(&workflow.Error{
		Idle: []*workflow.Task{task4},
		Failed: map[*workflow.Task]error{
			task1: context.Canceled,
			task2: context.Canceled,
		},
	}, err) {
		t.Fatalf("unexpected error details: %#v", err)
	}

	// check execution
	exp := (2 * 7) + 3 + 5 + 11
	if sum != exp {
		t.Fatalf("unexpected sum: want=%d got=%d", exp, sum)
	}
}

func TestCancelTask(t *testing.T) {
	sum := 2
	task1 := workflow.NewTask(t.Name()+"_1",
		workflow.RunnerFunc(func(context.Context) error {
			sum *= 3
			return nil
		}),
	)
	task2 := workflow.NewTask(t.Name()+"_2",
		workflow.RunnerFunc(func(ctx context.Context) error {
			sum += 5
			st := time.Now()
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
		workflow.RunnerFunc(func(ctx context.Context) error {
			sum += 11
			time.Sleep(15 * time.Millisecond)
			return nil
		}), task1,
	)
	task4 := workflow.NewTask(t.Name()+"_4",
		workflow.RunnerFunc(func(ctx context.Context) error {
			sum *= 13
			workflow.CancelTask(ctx, task2)
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

func TestAtExitPanic(t *testing.T) {
	defer func() {
		v := recover()
		if s, ok := v.(string); ok && s == "context.Context didn't bind to workflow context" {
			return
		}
		t.Fatalf("unexpected recover() returns: %v", v)
	}()
	workflow.AtExit(context.Background(), nil)
}

func TestCancelSelfTask(t *testing.T) {
	sum := 2
	var taskMe *workflow.Task
	task1 := workflow.NewTask(t.Name()+"_1",
		workflow.RunnerFunc(func(ctx context.Context) error {
			sum *= 3
			workflow.CancelTask(ctx, taskMe)
			// this may return context.Canceled if CancelTask is succeeded.
			// and the test would fail in that case.
			return ctx.Err()
		}),
	)
	taskMe = task1
	err := workflow.Run(context.Background(), task1)
	if err != nil {
		t.Fatal(err)
	}
	exp := 2 * 3
	if sum != exp {
		t.Fatalf("unexpected sum: want=%d got=%d", exp, sum)
	}
}

func TestError(t *testing.T) {
	task1 := workflow.NewTask(t.Name()+"_1",
		workflow.RunnerFunc(func(context.Context) error {
			return errors.New("planned error")
		}),
	)
	task2 := workflow.NewTask(t.Name()+"_2",
		workflow.RunnerFunc(func(context.Context) error {
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
		workflow.RunnerFunc(func(ctx context.Context) error {
			sum *= 3
			workflow.TaskSetResult(ctx, sum)
			return nil
		}),
	)
	task2 := workflow.NewTask(t.Name()+"_2",
		workflow.RunnerFunc(func(ctx context.Context) error {
			sum += 5
			workflow.TaskSetResult(ctx, sum)
			return nil
		}), task1,
	)
	task3 := workflow.NewTask(t.Name()+"_3",
		workflow.RunnerFunc(func(ctx context.Context) error {
			o1, err := workflow.Result(ctx, task1)
			if err != nil {
				return err
			}
			o2, err := workflow.Result(ctx, task2)
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

func TestInvalidOutput(t *testing.T) {
	errFoo := errors.New("foo")
	task1 := workflow.NewTask(t.Name()+"_1",
		workflow.RunnerFunc(func(context.Context) error {
			// no outputs
			return nil
		}),
	)
	task2 := workflow.NewTask(t.Name()+"_2",
		workflow.RunnerFunc(func(context.Context) error {
			// end with error
			return errFoo
		}),
	)
	task3 := workflow.NewTask(t.Name()+"_3",
		workflow.RunnerFunc(func(context.Context) error {
			// not in a workflow
			return nil
		}),
	)
	task0 := workflow.NewTask(t.Name()+"_0",
		workflow.RunnerFunc(func(ctx context.Context) error {
			_, err := workflow.Result(ctx, task1)
			if !errors.Is(err, workflow.ErrNoOutput) {
				t.Errorf("unexpected error for task1:\nwant=%s\ngot=%s", workflow.ErrNoOutput, err)
			}
			_, err = workflow.Result(ctx, task2)
			if !errors.Is(err, errFoo) {
				t.Errorf("unexpected error for task2:\nwant=%s\ngot=%s", errFoo, err)
			}
			_, err = workflow.Result(ctx, task3)
			if !errors.Is(err, workflow.ErrNotInWorkflow) {
				t.Errorf("unexpected error for task2:\nwant=%s\ngot=%s", workflow.ErrNotInWorkflow, err)
			}
			// verify outputs
			return nil
		}),
	).WhenComplete(task1).WhenEnd(task2)
	err := workflow.Run(context.Background(), task0)
	if err == nil {
		t.Fatal("workflow should be failed")
	}
	expErr := `workflow failed: 0 tasks not run, 1 tasks have error`
	if s := err.Error(); s != expErr {
		t.Fatalf("unexpected error:\nwant=%s\ngot=%s", expErr, s)
	}
}

func TestAtExit(t *testing.T) {
	sum := 2
	task1 := workflow.NewTask(t.Name()+"_1",
		workflow.RunnerFunc(func(ctx context.Context) error {
			workflow.AtExit(ctx, func(context.Context) {
				sum *= 3
			})
			return nil
		}),
	)
	task2 := workflow.NewTask(t.Name()+"_2",
		workflow.RunnerFunc(func(ctx context.Context) error {
			workflow.AtExit(ctx, func(context.Context) {
				sum += 5
			})
			return nil
		}), task1,
	)
	task3 := workflow.NewTask(t.Name()+"_3",
		workflow.RunnerFunc(func(ctx context.Context) error {
			workflow.AtExit(ctx, func(context.Context) {
				sum *= 7
			})
			return nil
		}), task2,
	)
	err := workflow.Run(context.Background(), task3)
	if err != nil {
		t.Fatal(err)
	}
	exp := (2*7 + 5) * 3
	if sum != exp {
		t.Fatalf("unexpected sum: want=%d got=%d", exp, sum)
	}
}

func TestWhenComplete(t *testing.T) {
	sum := 2
	task1 := workflow.NewTask(t.Name()+"_1",
		workflow.RunnerFunc(func(context.Context) error {
			sum *= 3
			return nil
		}),
	)
	task2 := workflow.NewTask(t.Name()+"_2",
		workflow.RunnerFunc(func(context.Context) error {
			sum += 5
			return nil
		}),
	)
	task3 := workflow.NewTask(t.Name()+"_3",
		workflow.RunnerFunc(func(context.Context) error {
			sum *= 7
			return nil
		}),
	)
	// reverse dependencies with TestSequential. and delay constructed.
	task1.WhenComplete(task2)
	task2.WhenComplete(task3)
	err := workflow.Run(context.Background(), task1)
	if err != nil {
		t.Fatal(err)
	}
	exp := (2*7 + 5) * 3
	if sum != exp {
		t.Fatalf("unexpected sum: want=%d got=%d", exp, sum)
	}
}

func TestWhenStart(t *testing.T) {
	// task1 -> (complete) -> task2
	// task2 -> (start) -> task3
	// task3 -> (complete) -> task4
	sum := 2
	task1 := workflow.NewTask(t.Name()+"_1",
		workflow.RunnerFunc(func(context.Context) error {
			sum *= 11
			return nil
		}),
	)
	task2 := workflow.NewTask(t.Name()+"_2",
		workflow.RunnerFunc(func(context.Context) error {
			sum += 3
			return nil
		}), task1,
	)
	task3 := workflow.NewTask(t.Name()+"_3",
		workflow.RunnerFunc(func(context.Context) error {
			sum += 5
			return nil
		}),
	).WhenStart(task2)
	task4 := workflow.NewTask(t.Name()+"_5",
		workflow.RunnerFunc(func(context.Context) error {
			sum += 7
			return nil
		}), task3,
	)
	err := workflow.Run(context.Background(), task4)
	if err != nil {
		t.Fatal(err)
	}
	exp := 2*11 + 3 + 5 + 7
	if sum != exp {
		t.Fatalf("unexpected sum: want=%d got=%d", exp, sum)
	}
}

func TestWhenEnd(t *testing.T) {
	task1 := workflow.NewTask(t.Name()+"_1",
		workflow.RunnerFunc(func(context.Context) error {
			return nil
		}),
	)
	task2 := workflow.NewTask(t.Name()+"_2",
		workflow.RunnerFunc(func(context.Context) error {
			return errors.New("foo")
		}), task1,
	)
	task3 := workflow.NewTask(t.Name()+"_3",
		workflow.RunnerFunc(func(context.Context) error {
			return nil
		}),
	).WhenEnd(task2)
	err := workflow.Run(context.Background(), task3)
	if err == nil {
		t.Fatal("workflow should be failed")
	}
	expErr := `workflow failed: 0 tasks not run, 1 tasks have error`
	if s := err.Error(); s != expErr {
		t.Fatalf("unexpected error:\nwant=%s\ngot=%s", expErr, s)
	}
}

func TestWorkflow_Add(t *testing.T) {
	sum := 2
	task1 := workflow.NewTask(t.Name()+"_1",
		workflow.RunnerFunc(func(context.Context) error {
			sum *= 3
			return nil
		}),
	)
	task2 := workflow.NewTask(t.Name()+"_2",
		workflow.RunnerFunc(func(context.Context) error {
			sum += 5
			return nil
		}), task1,
	)
	task3 := workflow.NewTask(t.Name()+"_3",
		workflow.RunnerFunc(func(context.Context) error {
			sum *= 7
			return nil
		}), task2,
	)
	err := workflow.New().Add(task3).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	exp := (2*3 + 5) * 7
	if sum != exp {
		t.Fatalf("unexpected sum: want=%d got=%d", exp, sum)
	}
}

func TestLogger(t *testing.T) {
	sum := 2
	task1 := workflow.NewTask(t.Name()+"_1",
		workflow.RunnerFunc(func(context.Context) error {
			sum *= 3
			return nil
		}),
	)
	w := workflow.New(task1)

	// 1st attempt with valid logger.
	bb := &bytes.Buffer{}
	l := log.New(bb, "", 0)
	err := w.SetLogger(l).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	exp := 2 * 3
	if sum != exp {
		t.Fatalf("unexpected sum: want=%d got=%d", exp, sum)
	}
	expLog := "[workflow.task:TestLogger_1] start\n[workflow.task:TestLogger_1] end\n"
	if s := bb.String(); s != expLog {
		t.Fatalf("unexpected log:\nwant=%q\ngot=%q", expLog, s)
	}

	// 2nd attempt with nil logger (fallback to discardLogger).
	bb.Reset()
	err = w.SetLogger(nil).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	exp = 2 * 3 * 3
	if sum != exp {
		t.Fatalf("unexpected sum: want=%d got=%d", exp, sum)
	}
	// logger in 1st atttempt is not used anymore.
	expLog = ""
	if s := bb.String(); s != expLog {
		t.Fatalf("unexpected log:\nwant=%q\ngot=%q", expLog, s)
	}
}

func TestNilRunner(t *testing.T) {
	task1 := workflow.NewTask(t.Name()+"_1", nil)
	w := workflow.New(task1)
	bb := &bytes.Buffer{}
	l := log.New(bb, "", 0)
	err := w.SetLogger(l).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	exp := "[workflow.task:TestNilRunner_1] start\n[workflow.task:TestNilRunner_1] end\n"
	if s := bb.String(); s != exp {
		t.Fatalf("unexpected log:\nwant=%q\ngot=%q", exp, s)
	}
}
