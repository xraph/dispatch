package runtime_test

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
)

func TestQueryRejectsSDKMutations(t *testing.T) {
	for _, mode := range []string{"activity", "options", "invalid_options", "timer", "signal", "future", "registration"} {
		for _, recoverAttempt := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%t", mode, recoverAttempt), func(t *testing.T) {
				f := newHistory()
				handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
					future := w.Activity("work", "work", "", nil)
					w.SetQueryHandler("status", func(_ []byte) ([]byte, error) {
						attempt := func() {
							switch mode {
							case "activity":
								w.Activity("new", "new", "", nil)
							case "options":
								w.ActivityWithOptions("new", "new", "", nil, drt.ActivityOptions{StartToCloseTimeout: time.Minute})
							case "invalid_options":
								w.ActivityWithOptions("new", "new", "", nil, drt.ActivityOptions{StartToCloseTimeout: -1})
							case "timer":
								w.Timer("new", time.Second)
							case "signal":
								w.ReceiveSignal("new", "approve")
							case "future":
								_, _ = future.Get()
							case "registration":
								w.SetQueryHandler("another", func(_ []byte) ([]byte, error) { return nil, nil })
							}
						}
						if recoverAttempt {
							func() { defer func() { _ = recover() }(); attempt() }()
						} else {
							attempt()
						}
						return []byte("mutation hidden"), nil
					})
					return future.Get()
				}
				result, err := drt.EvaluateQuery(f.execution, f.events, handler, queryRequest(f, "status"))
				if !errors.Is(err, drt.ErrQueryMutation) || len(result.Output) != 0 || result.Revision != 0 {
					t.Fatalf("query mutation escaped: %+v %v", result, err)
				}
			})
		}
	}
}

func TestQueryRegistrationAndErrors(t *testing.T) {
	applicationErr := errors.New("query application error")
	for _, mode := range []string{"empty", "long", "nil", "duplicate", "missing", "panic", "error", "late"} {
		t.Run(mode, func(t *testing.T) {
			f := newHistory()
			want := durable.ErrInvalid
			handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
				name := "status"
				var query drt.QueryFunc = func(_ []byte) ([]byte, error) { return nil, nil }
				switch mode {
				case "empty":
					name = ""
				case "long":
					name = strings.Repeat("n", 201)
				case "nil":
					query = nil
				case "missing":
					name = "other"
				case "panic":
					query = func(_ []byte) ([]byte, error) { panic("query panic") }
				case "error":
					query = func(_ []byte) ([]byte, error) { return []byte("discard"), applicationErr }
				case "late":
					_, _ = w.ReceiveSignal("approval", "approve").Get()
				}
				w.SetQueryHandler(name, query)
				if mode == "duplicate" {
					w.SetQueryHandler(name, query)
				}
				return nil, nil
			}
			switch mode {
			case "missing", "late":
				want = drt.ErrQueryNotFound
			case "panic":
				want = drt.ErrQueryPanic
			case "error":
				want = applicationErr
			}
			got, err := drt.EvaluateQuery(f.execution, f.events, handler, queryRequest(f, "status"))
			if !errors.Is(err, want) || len(got.Output) != 0 {
				t.Fatalf("query error: %+v %v", got, err)
			}
		})
	}
}

func TestQueryRequestBounds(t *testing.T) {
	f := newHistory()
	base := queryRequest(f, "status")
	for _, mode := range []string{"valid", "name_limit", "name_excess", "build_limit", "build_excess", "key_limit", "key_excess", "input_limit", "input_excess", "run_missing", "blank", "nul", "utf8"} {
		t.Run(mode, func(t *testing.T) {
			request := base
			valid := false
			switch mode {
			case "valid":
				valid = true
			case "name_limit":
				request.Name = strings.Repeat("n", 200)
				valid = true
			case "name_excess":
				request.Name = strings.Repeat("n", 201)
			case "build_limit":
				request.BuildID = strings.Repeat("b", 512)
				valid = true
			case "build_excess":
				request.BuildID = strings.Repeat("b", 513)
			case "key_limit":
				request.WorkflowID = strings.Repeat("w", 512)
				valid = true
			case "key_excess":
				request.WorkflowID = strings.Repeat("w", 513)
			case "input_limit":
				request.Input = make([]byte, 1<<20)
				valid = true
			case "input_excess":
				request.Input = make([]byte, (1<<20)+1)
			case "run_missing":
				request.RunID = ""
			case "blank":
				request.Name = " "
			case "nul":
				request.Name = "a\x00b"
			case "utf8":
				request.Name = string([]byte{255})
			}
			err := request.Validate()
			if valid && err != nil || !valid && !errors.Is(err, durable.ErrInvalid) {
				t.Fatalf("query validation: %v", err)
			}
		})
	}
	handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
		w.SetQueryHandler("status", func(_ []byte) ([]byte, error) { return nil, nil })
		return nil, nil
	}
	for _, field := range []string{"namespace", "workflow", "run", "build"} {
		request := base
		switch field {
		case "namespace":
			request.Namespace = "other"
		case "workflow":
			request.WorkflowID = "other"
		case "run":
			request.RunID = "other"
		case "build":
			request.BuildID = "other"
		}
		if _, err := drt.EvaluateQuery(f.execution, f.events, handler, request); !errors.Is(err, durable.ErrInvalid) {
			t.Fatalf("query snapshot mismatch %s: %v", field, err)
		}
	}
}

func TestQueryCopiesPayloadsAndIsolatesCalls(t *testing.T) {
	f := newHistory()
	request := queryRequest(f, "status")
	request.Input = []byte("input")
	shared := []byte("result")
	handler := func(w *drt.Workflow, input []byte) ([]byte, error) {
		input[0] = 'X'
		count := 0
		w.SetQueryHandler("status", func(queryInput []byte) ([]byte, error) {
			queryInput[0] = 'Y'
			count++
			if count != 1 {
				return nil, errors.New("query state leaked")
			}
			return shared, nil
		})
		return w.ReceiveSignal("approval", "approve").Get()
	}
	first, err := drt.EvaluateQuery(f.execution, f.events, handler, request)
	if err != nil {
		t.Fatal(err)
	}
	shared[0] = 'Z'
	if string(first.Output) != "result" || string(request.Input) != "input" || string(f.execution.Input) != "order-input" {
		t.Fatalf("query aliases inputs or output: %q %q %q", first.Output, request.Input, f.execution.Input)
	}
	if _, err = drt.EvaluateQuery(f.execution, f.events, handler, request); err != nil {
		t.Fatalf("query reused mutable instance: %v", err)
	}
	for _, size := range []int{1 << 20, (1 << 20) + 1} {
		handler = func(w *drt.Workflow, _ []byte) ([]byte, error) {
			w.SetQueryHandler("status", func(_ []byte) ([]byte, error) { return bytes.Repeat([]byte("x"), size), nil })
			return nil, nil
		}
		result, queryErr := drt.EvaluateQuery(f.execution, f.events, handler, request)
		if size == 1<<20 {
			if queryErr != nil || len(result.Output) != size {
				t.Fatalf("valid query output: %d %v", len(result.Output), queryErr)
			}
		} else if !errors.Is(queryErr, durable.ErrInvalid) || len(result.Output) != 0 {
			t.Fatalf("excess query output: %d %v", len(result.Output), queryErr)
		}
	}
}
