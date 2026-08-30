package preflight

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRunStopsAtTheFirstFailure(t *testing.T) {
	var ran []string
	checks := []Check{
		{Name: "first", Invariant: "a is true", Hint: "look at a",
			Fn: func(context.Context) error { ran = append(ran, "first"); return nil }},
		{Name: "second", Invariant: "b is true", Hint: "look at b",
			Fn: func(context.Context) error { ran = append(ran, "second"); return errors.New("b was false") }},
		{Name: "third", Invariant: "c is true", Hint: "look at c",
			Fn: func(context.Context) error { ran = append(ran, "third"); return nil }},
	}

	var buf bytes.Buffer
	results, err := Run(context.Background(), &buf, checks)
	if err == nil {
		t.Fatal("expected the run to fail")
	}
	if len(ran) != 2 {
		t.Errorf("ran %v, want the run to stop before the third check", ran)
	}
	if len(results) != 2 || !results[0].Passed || results[1].Passed {
		t.Errorf("results = %+v, want first passed and second failed", results)
	}

	var f *Failure
	if !errors.As(err, &f) {
		t.Fatalf("error is not a *Failure: %v", err)
	}
	msg := f.Error()
	for _, want := range []string{`"second"`, "b was false", "invariant: b is true", "check: look at b"} {
		if !strings.Contains(msg, want) {
			t.Errorf("failure message is missing %q:\n%s", want, msg)
		}
	}

	out := buf.String()
	if !strings.Contains(out, "[ ok ] first") || !strings.Contains(out, "[FAIL] second") {
		t.Errorf("progress output is wrong:\n%s", out)
	}
	if strings.Contains(out, "third") {
		t.Error("a check after the failure must not be reported")
	}
}

func TestRunPassesEverything(t *testing.T) {
	checks := []Check{
		{Name: "a", Fn: func(context.Context) error { return nil }},
		{Name: "b", Fn: func(context.Context) error { return nil }},
	}
	results, err := Run(context.Background(), &bytes.Buffer{}, checks)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 || !results[0].Passed || !results[1].Passed {
		t.Errorf("results = %+v", results)
	}
}

func TestWaitForReturnsOnceTheConditionHolds(t *testing.T) {
	calls := 0
	err := WaitFor(context.Background(), time.Second, time.Millisecond, "the thing",
		func(context.Context) (bool, string, error) {
			calls++
			return calls >= 3, "not yet", nil
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 3 {
		t.Errorf("cond called %d times, want 3", calls)
	}
}

// TestWaitForTimeoutIncludesTheLastObservation: a timeout must say what was
// still not true, not merely that time ran out.
func TestWaitForTimeoutIncludesTheLastObservation(t *testing.T) {
	err := WaitFor(context.Background(), 30*time.Millisecond, 5*time.Millisecond, "probe pod readiness",
		func(context.Context) (bool, string, error) {
			return false, "pod drainwatch-probe-abc Ready=False reason=\"ContainersNotReady\"", nil
		})
	if err == nil {
		t.Fatal("expected a timeout")
	}
	if !strings.Contains(err.Error(), "ContainersNotReady") {
		t.Errorf("timeout must carry the last observation, got: %v", err)
	}
	if !strings.Contains(err.Error(), "probe pod readiness") {
		t.Errorf("timeout must name what was being waited on, got: %v", err)
	}
}

func TestWaitForPropagatesConditionErrors(t *testing.T) {
	sentinel := errors.New("the api said no")
	err := WaitFor(context.Background(), time.Second, time.Millisecond, "the thing",
		func(context.Context) (bool, string, error) { return false, "", sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("got %v, want the underlying error to be wrapped", err)
	}
}

func TestWaitForHonoursCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := WaitFor(ctx, time.Minute, time.Millisecond, "the thing",
		func(context.Context) (bool, string, error) { return false, "nope", nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}
