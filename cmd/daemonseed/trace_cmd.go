package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/arterberry/daemonseed/internal/trace"
)

// newTraceCmd implements `daemonseed trace`: a viewer over the session
// trace, working against either backend. It reads the store directly, so it
// works whether or not the daemon is running.
func newTraceCmd() *cobra.Command {
	var (
		lines   int
		session string
		traceID string
		kind    string
	)
	cmd := &cobra.Command{
		Use:   "trace",
		Short: "Show recent session trace events (tool calls and bus messages)",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			if !cfg.Trace.Enabled {
				fmt.Fprintln(os.Stderr, "note: tracing is disabled in config; showing whatever was previously recorded")
			}

			var events []trace.Event
			switch cfg.Trace.Backend {
			case trace.BackendSQLite:
				store, err := trace.NewSQLiteStore(trace.ResolvePath(cfg.Trace.Backend, cfg.Trace.Path))
				if err != nil {
					return err
				}
				defer store.Close()
				events, err = store.Recent(lines, session, traceID)
				if err != nil {
					return err
				}
			default:
				events, err = trace.ReadJSONL(cfg.Trace.Path, lines, session, traceID)
				if err != nil {
					if os.IsNotExist(err) {
						fmt.Println("no trace recorded yet")
						return nil
					}
					return err
				}
			}

			for _, ev := range events {
				if kind != "" && ev.Kind != kind {
					continue
				}
				fmt.Println(formatTraceEvent(ev))
			}
			return nil
		},
	}
	cmd.Flags().IntVarP(&lines, "lines", "n", 50, "number of trailing events to show")
	cmd.Flags().StringVar(&session, "session", "", "filter by client name")
	cmd.Flags().StringVar(&traceID, "trace-id", "", "filter by trace id (follow one request/response chain)")
	cmd.Flags().StringVar(&kind, "kind", "", "filter by kind: message, tool, fire, lifecycle")
	return cmd
}

func formatTraceEvent(ev trace.Event) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s  %-9s %-22s", ev.TS.Local().Format("15:04:05.000"), ev.Kind, ev.Name)
	switch {
	case ev.Kind == trace.KindTool:
		fmt.Fprintf(&b, " %s(%s)", ev.Session, ev.Role)
		if ev.DurationMs > 0 {
			fmt.Fprintf(&b, " %.1fms", ev.DurationMs)
		}
	case ev.From != "" || ev.To != "":
		fmt.Fprintf(&b, " %s→%s", ev.From, ev.To)
	case ev.Session != "":
		fmt.Fprintf(&b, " %s", ev.Session)
	}
	if ev.Status != "" && ev.Status != trace.StatusOK {
		fmt.Fprintf(&b, " [%s]", strings.ToUpper(ev.Status))
	}
	if ev.TraceID != "" {
		fmt.Fprintf(&b, " trace=%.8s", ev.TraceID)
	}
	if ev.Detail != "" {
		fmt.Fprintf(&b, "  %s", ev.Detail)
	}
	return b.String()
}
