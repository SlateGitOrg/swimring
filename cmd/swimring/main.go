// Command swimring runs SWIM clusters: deterministic simulations and
// evaluations, an end-to-end demo, and real UDP nodes.
//
// Usage:
//
//	swimring sim   [flags]   simulate one scenario and print its timeline
//	swimring eval  [flags]   run the seeded experiments, write markdown tables
//	swimring demo            offline end-to-end demo (simulated + loopback UDP)
//	swimring node  [flags]   run a real UDP node with an interactive shell
package main

import (
	"errors"
	"fmt"
	"io"
	"os"
)

const usage = `swimring — SWIM membership + consistent-hash ring + replicated cache

usage:
  swimring sim   [flags]   simulate one scenario and print its timeline
  swimring eval  [flags]   run the seeded experiments and write markdown tables
  swimring demo  [flags]   offline end-to-end demo (simulated cluster + loopback UDP)
  swimring node  [flags]   run a real UDP node with an interactive shell

run "swimring <command> -h" for the flags of a command.
`

var errUsage = errors.New("usage")

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr); err != nil {
		if !errors.Is(err, errUsage) {
			fmt.Fprintln(os.Stderr, "swimring:", err)
		}
		os.Exit(2)
	}
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return errUsage
	}
	switch args[0] {
	case "sim":
		return runSim(args[1:], stdout, stderr)
	case "eval":
		return runEval(args[1:], stdout, stderr)
	case "demo":
		return runDemo(args[1:], stdout, stderr)
	case "node":
		return runNode(args[1:], stdin, stdout, stderr)
	case "-h", "--help", "help":
		fmt.Fprint(stdout, usage)
		return nil
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n%s", args[0], usage)
		return errUsage
	}
}
