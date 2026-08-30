// Package preflight runs the checks that must pass before drainwatch is allowed
// to measure anything.
//
// The contract is deliberately blunt: every check runs in order, the first
// failure aborts the run, and the failure message names the check, the
// invariant it enforces and what to look at. A harness that is broken must
// abort rather than emit a report, because a report that looks complete but was
// produced by a broken harness is worse than no report at all.
package preflight

import (
	"context"
	"fmt"
	"io"
	"time"
)

// Check is one precondition.
type Check struct {
	// Name is the short identifier printed in the check list.
	Name string
	// Invariant states, in one sentence, what must be true.
	Invariant string
	// Hint tells the operator what to look at when it is not.
	Hint string
	// Fn returns nil when the invariant holds.
	Fn func(ctx context.Context) error
}

// Failure is the error returned when a check does not pass. Its message is the
// whole diagnosis: which check, which invariant, what to check, and the
// underlying error.
type Failure struct {
	Check Check
	Err   error
}

func (f *Failure) Error() string {
	return fmt.Sprintf("preflight check %q failed: %v\n  invariant: %s\n  check: %s",
		f.Check.Name, f.Err, f.Check.Invariant, f.Check.Hint)
}

func (f *Failure) Unwrap() error { return f.Err }

// Result records the outcome of one check, for inclusion in the run log.
type Result struct {
	Name     string
	Passed   bool
	Duration time.Duration
	Err      error
}

// Run executes checks in order, printing progress to out, and stops at the
// first failure. The returned results cover every check that was attempted.
func Run(ctx context.Context, out io.Writer, checks []Check) ([]Result, error) {
	results := make([]Result, 0, len(checks))
	for _, c := range checks {
		start := time.Now()
		err := c.Fn(ctx)
		d := time.Since(start)
		results = append(results, Result{Name: c.Name, Passed: err == nil, Duration: d, Err: err})
		if err != nil {
			fmt.Fprintf(out, "preflight [FAIL] %-34s %6dms\n", c.Name, d.Milliseconds())
			return results, &Failure{Check: c, Err: err}
		}
		fmt.Fprintf(out, "preflight [ ok ] %-34s %6dms\n", c.Name, d.Milliseconds())
	}
	return results, nil
}

// WaitFor polls cond until it returns true, the context is done, or timeout
// elapses. The description and the last observed reason are folded into the
// timeout error so that a failure says what was still not true.
func WaitFor(ctx context.Context, timeout, interval time.Duration, description string, cond func(context.Context) (bool, string, error)) error {
	deadline := time.Now().Add(timeout)
	tick := time.NewTicker(interval)
	defer tick.Stop()

	lastReason := "no observation yet"
	for {
		ok, reason, err := cond(ctx)
		if err != nil {
			return fmt.Errorf("%s: %w", description, err)
		}
		if ok {
			return nil
		}
		if reason != "" {
			lastReason = reason
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s did not become true within %s; last observation: %s", description, timeout, lastReason)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: cancelled: %w", description, ctx.Err())
		case <-tick.C:
		}
	}
}
