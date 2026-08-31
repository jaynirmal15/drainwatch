// Command drainwatch measures what happens to established TCP and UDP flows
// when a Kubernetes pod terminates, and records the answer as evidence.
//
// One binary, three subcommands:
//
//	drainwatch run     the orchestrator: deploy, verify, hold, trigger, observe, report
//	drainwatch probe   the in-cluster workload whose termination is measured
//	drainwatch client  the flow generator, exposed for manual runs
//
// and one reporting subcommand:
//
//	drainwatch aggregate  min/median/max across the repeats of an arm
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/jaynirmal15/drainwatch/internal/flowgen"
	"github.com/jaynirmal15/drainwatch/internal/orchestrate"
	"github.com/jaynirmal15/drainwatch/internal/probe"
	"github.com/jaynirmal15/drainwatch/internal/report"
)

// Exit codes. 2 is reserved for "you asked for something this version does not
// do", so that scripts can distinguish it from a measurement failure.
const (
	exitOK             = 0
	exitError          = 1
	exitNotImplemented = 2
)

func main() {
	if len(os.Args) < 2 {
		usage(os.Stderr)
		os.Exit(exitError)
	}

	switch os.Args[1] {
	case "run":
		os.Exit(cmdRun(os.Args[2:]))
	case "probe":
		os.Exit(cmdProbe(os.Args[2:]))
	case "client":
		os.Exit(cmdClient(os.Args[2:]))
	case "aggregate":
		os.Exit(cmdAggregate(os.Args[2:]))
	case "version", "--version", "-version":
		fmt.Printf("drainwatch %s (%s)\n", report.Version, report.GitCommit)
		os.Exit(exitOK)
	case "help", "--help", "-h":
		usage(os.Stdout)
		os.Exit(exitOK)
	default:
		fmt.Fprintf(os.Stderr, "drainwatch: unknown subcommand %q\n\n", os.Args[1])
		usage(os.Stderr)
		os.Exit(exitError)
	}
}

func usage(w *os.File) {
	fmt.Fprintf(w, `drainwatch %s (%s)

Measure what happens to established TCP and UDP flows when a pod terminates.

Usage:
  drainwatch run     [flags]   orchestrate a full trial and write a report
  drainwatch probe   [flags]   run the in-cluster test workload
  drainwatch client  [flags]   run the flow generator by hand
  drainwatch aggregate <dirs>  summarise the repeats of one or more arm directories
  drainwatch version           print the version and git commit

Run "drainwatch <subcommand> --help" for the flags of a subcommand.
`, report.Version, report.GitCommit)
}

// signalContext returns a context cancelled on SIGINT/SIGTERM, so that an
// interrupted run tears its flows down instead of leaving sockets open.
func signalContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-ch
		fmt.Fprintln(os.Stderr, "drainwatch: interrupt received; tearing down flows")
		cancel()
	}()
	return ctx, cancel
}

func cmdRun(args []string) int {
	fs := flag.NewFlagSet("drainwatch run", flag.ExitOnError)
	var o orchestrate.Options

	fs.StringVar(&o.Kubeconfig, "kubeconfig", "", "path to a kubeconfig (default: $KUBECONFIG, then ~/.kube/config)")
	fs.StringVar(&o.Context, "context", "", "kubeconfig context to use (default: the current context)")
	fs.StringVar(&o.Namespace, "namespace", "drainwatch", "namespace to deploy the probe into; created if absent")
	fs.StringVar(&o.ManifestPath, "manifest", filepath.Join("deploy", "manifests", "probe.yaml"), "path to the probe Deployment+Service manifest")
	fs.StringVar(&o.Image, "image", "drainwatch-probe:"+report.Version, "probe container image")
	fs.StringVar(&o.Workload, "workload", orchestrate.WorkloadDeploy, "deploy | attach (attach is not implemented in v0.1)")
	fs.StringVar(&o.Trigger, "trigger", orchestrate.TriggerDelete, "delete | evict | scale")
	fs.StringVar(&o.DrainBehavior, "drain-behavior", probe.BehaviorDrain, "drain | exit-now | ignore (sets DRAIN_BEHAVIOR on the probe)")
	fs.IntVar(&o.GracePeriod, "grace-period", 30, "terminationGracePeriodSeconds for the probe pod")
	fs.IntVar(&o.DrainMaxSeconds, "drain-max-seconds", 0, "probe DRAIN_MAX_SECONDS; 0 means auto (grace-period minus 5s)")
	fs.BoolVar(&o.ExitNowForceRST, "exit-now-force-rst", true, "under --drain-behavior exit-now, close established flows with SO_LINGER=0 so abandonment is a deterministic RST")
	fs.IntVar(&o.TCPFlows, "tcp-flows", flowgen.DefaultTCPFlows, "number of long-lived TCP flows")
	fs.IntVar(&o.UDPFlows, "udp-flows", flowgen.DefaultUDPFlows, "number of UDP flows")
	fs.StringVar(&o.TargetHost, "target-host", "127.0.0.1", "host to reach the probe on (kind maps NodePorts to localhost)")
	fs.IntVar(&o.TCPPort, "tcp-port", 7001, "host port that reaches the probe's TCP listener")
	fs.IntVar(&o.UDPPort, "udp-port", 7002, "host port that reaches the probe's UDP listener")
	fs.IntVar(&o.SettleSeconds, "settle-seconds", 10, "how long to hold flows in steady state before triggering")
	fs.IntVar(&o.ObserveTimeoutSeconds, "observe-timeout", 0, "observation window in seconds; 0 means auto (grace-period + 30)")
	fs.IntVar(&o.ReadyTimeoutSeconds, "ready-timeout", 120, "how long to wait for the probe pod to become Ready")
	fs.IntVar(&o.PostTerminalGraceSecond, "post-terminal-grace", 5, "how long to keep watching after the last flow ends, to catch trailing cluster events")
	fs.DurationVar(&o.FlowTimeout, "flow-timeout", flowgen.DefaultFlowTimeout, "TCP read timeout before a flow is recorded as read-timeout")
	fs.DurationVar(&o.Interval, "interval", flowgen.DefaultInterval, "heartbeat and datagram interval")
	fs.IntVar(&o.UDPSilenceDatagrams, "udp-silence-datagrams", flowgen.DefaultUDPSilenceDatagrams, "consecutive unanswered datagrams before a UDP flow is declared severed")
	fs.IntVar(&o.Repeat, "repeat", 1, "number of trials to run into --out")
	fs.StringVar(&o.OutDir, "out", "out", "output directory for report.json and summary.json")

	if err := fs.Parse(args); err != nil {
		return exitError
	}

	ctx, cancel := signalContext()
	defer cancel()

	if err := orchestrate.Run(ctx, os.Stdout, o); err != nil {
		if errors.Is(err, orchestrate.ErrAttachNotImplemented) {
			fmt.Fprintln(os.Stderr, "drainwatch: --workload attach: not implemented in v0.1")
			fmt.Fprintln(os.Stderr, "  v0.1 measures a workload it deploys itself, so that the probe's SIGTERM handling and readiness")
			fmt.Fprintln(os.Stderr, "  behaviour are known quantities. Attaching to an arbitrary existing workload is out of scope.")
			return exitNotImplemented
		}
		fmt.Fprintf(os.Stderr, "drainwatch: %v\n", err)
		return exitError
	}
	return exitOK
}

func cmdProbe(args []string) int {
	fs := flag.NewFlagSet("drainwatch probe", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), `Usage: drainwatch probe

The in-cluster test workload. Configured entirely by environment variable so
that the Deployment manifest is the single source of truth:

  DRAIN_BEHAVIOR       drain (default) | exit-now | ignore
  DRAIN_MAX_SECONDS    drain window in seconds (default %d)
  EXIT_NOW_FORCE_RST   under exit-now, close flows with SO_LINGER=0 (default true)
  PROBE_TCP_PORT       default 7001
  PROBE_UDP_PORT       default 7002
  PROBE_READY_PORT     default 7003 (serves /readyz and /healthz)

Emits one JSON event per line on stdout. These lines are how SIGTERM delivery
time enters the report timeline.
`, probe.DefaultDrainMaxSeconds)
	}
	if err := fs.Parse(args); err != nil {
		return exitError
	}

	cfg, err := probe.ConfigFromEnv(os.Getenv)
	if err != nil {
		// A probe running a different behaviour from the one the report claims
		// would contaminate every result, so a bad environment is fatal.
		fmt.Fprintf(os.Stderr, "drainwatch probe: %v\n", err)
		return exitError
	}

	log := probe.NewLogger(os.Stdout, time.Now())
	srv := probe.NewServer(cfg, log)

	// The probe installs its own SIGTERM handler; this context exists only so
	// that the ignore behaviour has something to block on.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	return srv.Run(ctx)
}

func cmdClient(args []string) int {
	fs := flag.NewFlagSet("drainwatch client", flag.ExitOnError)
	host := fs.String("host", "127.0.0.1", "host running the probe")
	tcpPort := fs.Int("tcp-port", 7001, "probe TCP port")
	udpPort := fs.Int("udp-port", 7002, "probe UDP port")
	tcpFlows := fs.Int("tcp-flows", flowgen.DefaultTCPFlows, "number of long-lived TCP flows")
	udpFlows := fs.Int("udp-flows", flowgen.DefaultUDPFlows, "number of UDP flows")
	flowTimeout := fs.Duration("flow-timeout", flowgen.DefaultFlowTimeout, "TCP read timeout before a flow is recorded as read-timeout")
	interval := fs.Duration("interval", flowgen.DefaultInterval, "heartbeat and datagram interval")
	silence := fs.Int("udp-silence-datagrams", flowgen.DefaultUDPSilenceDatagrams, "consecutive unanswered datagrams before a UDP flow is declared severed")
	duration := fs.Duration("duration", 30*time.Second, "how long to hold the flows before closing the observation window")
	outPath := fs.String("out", "", "optional path to write the flow records as JSON")

	if err := fs.Parse(args); err != nil {
		return exitError
	}

	cfg := flowgen.Config{
		Host:                *host,
		TCPPort:             *tcpPort,
		UDPPort:             *udpPort,
		TCPFlows:            *tcpFlows,
		UDPFlows:            *udpFlows,
		Interval:            *interval,
		GapThreshold:        flowgen.DefaultGapThreshold,
		FlowTimeout:         *flowTimeout,
		UDPSilenceDatagrams: *silence,
		DialTimeout:         flowgen.DefaultDialTimeout,
	}

	gen, err := flowgen.New(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "drainwatch client: %v\n", err)
		return exitError
	}

	ctx, cancel := signalContext()
	defer cancel()

	t0 := time.Now()
	if err := gen.Start(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "drainwatch client: %v\n", err)
		return exitError
	}
	if err := gen.WaitEstablished(ctx, 30*time.Second, ""); err != nil {
		gen.CloseObservation(time.Now())
		fmt.Fprintf(os.Stderr, "drainwatch client: %v\n", err)
		return exitError
	}
	fmt.Fprintf(os.Stdout, "established %d tcp and %d udp flows against %s; holding for %s\n",
		*tcpFlows, *udpFlows, *host, *duration)

	deadline := time.After(*duration)
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-deadline:
			break loop
		case <-tick.C:
			if gen.AllTerminal() {
				fmt.Fprintln(os.Stdout, "every flow reached a terminal event; stopping early")
				break loop
			}
		}
	}

	gen.CloseObservation(time.Now())
	flows := gen.Records(t0)
	timeline := gen.TimelineEvents(t0)
	report.SortTimeline(timeline)
	summary := report.BuildSummary(timeline, flows)

	fmt.Fprintln(os.Stdout)
	report.RenderFlowTable(os.Stdout, flows)
	fmt.Fprintln(os.Stdout)
	report.RenderSummaryTable(os.Stdout, summary)

	if *outPath != "" {
		trial := report.Trial{
			ID: "client-manual",
			Config: report.Config{
				TCPFlows:            *tcpFlows,
				UDPFlows:            *udpFlows,
				TargetHost:          *host,
				TCPPort:             *tcpPort,
				UDPPort:             *udpPort,
				FlowTimeoutSeconds:  flowTimeout.Seconds(),
				UDPSilenceDatagrams: *silence,
				Trigger:             "none (manual client run)",
				Workload:            "none (manual client run)",
			},
			Timeline:  timeline,
			Flows:     flows,
			Summary:   summary,
			ClockNote: report.ClockNoteClientText,
		}
		out := report.Report{
			DrainwatchVersion: report.Version,
			GitCommit:         report.GitCommit,
			Trial:             trial,
		}
		if err := report.WriteJSON(*outPath, out); err != nil {
			fmt.Fprintf(os.Stderr, "drainwatch client: %v\n", err)
			return exitError
		}
		fmt.Fprintf(os.Stdout, "\nflow records written to %s\n", *outPath)
	}
	return exitOK
}

// cmdAggregate summarises the repeats of one or more arm directories.
//
// An arm directory holds trial-NNN/report.json for each repeat, which is the
// layout `drainwatch run --repeat N --out <dir>` produces.
func cmdAggregate(args []string) int {
	fs := flag.NewFlagSet("drainwatch aggregate", flag.ExitOnError)
	outPath := fs.String("out", "", "optional path to write the aggregate as JSON")
	strict := fs.Bool("strict", false, "exit 3 if any arm's repeats disagree with each other")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), `Usage: drainwatch aggregate [flags] <arm-dir>...

Reads trial-NNN/report.json from each arm directory and reports, per arm:
  * min / median / max for each interval, across the repeats
  * per-repeat outcome counts and mechanisms
  * container exit codes

Same-clock and cross-clock intervals are reported in separate blocks and are
never combined into one statistic: they do not have the same precision.

A metric no repeat observed is reported as not-measured, never as zero. If the
repeats of an arm disagree on any outcome, mechanism, exit code, configuration
or environment, that is announced above the statistics rather than averaged in.`)
		fmt.Fprintln(fs.Output())
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return exitError
	}
	dirs := fs.Args()
	if len(dirs) == 0 {
		fmt.Fprintln(os.Stderr, "drainwatch aggregate: no arm directories given")
		fs.Usage()
		return exitError
	}

	arms, err := report.LoadArms(dirs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "drainwatch aggregate: %v\n", err)
		return exitError
	}

	for i, a := range arms {
		if i > 0 {
			fmt.Fprintln(os.Stdout)
		}
		report.RenderArmAggregate(os.Stdout, a)
	}
	if len(arms) > 1 {
		fmt.Fprintln(os.Stdout)
		report.RenderMatrix(os.Stdout, arms)
	}

	if *outPath != "" {
		if err := report.WriteJSON(*outPath, arms); err != nil {
			fmt.Fprintf(os.Stderr, "drainwatch aggregate: %v\n", err)
			return exitError
		}
		fmt.Fprintf(os.Stdout, "\naggregate written to %s\n", *outPath)
	}

	if *strict {
		for _, a := range arms {
			if a.HasDisagreements {
				fmt.Fprintf(os.Stderr, "drainwatch aggregate: arm %s has repeats that disagree (--strict)\n", a.Arm)
				return 3
			}
		}
	}
	return exitOK
}
