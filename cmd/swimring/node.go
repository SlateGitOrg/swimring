package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/SlateGitOrg/swimring"
	"github.com/SlateGitOrg/swimring/cache"
	"github.com/SlateGitOrg/swimring/clock"
	"github.com/SlateGitOrg/swimring/swim"
	"github.com/SlateGitOrg/swimring/transport"
)

const shellHelp = `commands:
  members            list known members
  put <key> <value>  write through the cache (N=3, W=2)
  get <key>          read through the cache (R=2)
  del <key>          delete a key
  owners <key>       show the ring's replicas for a key
  stats              protocol and cache counters
  leave              leave gracefully and exit
  quit               exit without leaving (peers will detect a failure)
`

func runNode(args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("node", flag.ContinueOnError)
	fs.SetOutput(stderr)
	name := fs.String("name", "", "member name (required, unique in the cluster)")
	bind := fs.String("bind", "127.0.0.1:7946", "UDP address to bind")
	join := fs.String("join", "", "comma-separated seed addresses")
	interval := fs.Duration("interval", 200*time.Millisecond, "probe interval")
	verbose := fs.Bool("v", false, "log protocol debug messages to stderr")
	if err := fs.Parse(args); err != nil {
		return errUsage
	}
	if *name == "" {
		fmt.Fprintln(stderr, "node: -name is required")
		return errUsage
	}
	level := slog.LevelWarn
	if *verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: level}))
	tr, err := transport.ListenUDP(*bind, logger)
	if err != nil {
		return err
	}
	cfg := fastConfig(*name)
	cfg.ProbeInterval = *interval
	cfg.ProbeTimeout = *interval * 2 / 5
	cfg.Logger = logger
	events := make(chan swim.Event, 64)
	cfg.Events = func(ev swim.Event) {
		select {
		case events <- ev:
		default:
		}
	}
	n, err := swimring.New(swimring.Options{
		SWIM: cfg, Cache: cache.DefaultConfig(), Transport: tr,
		Clock: clock.NewReal(), Rand: rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0)),
	})
	if err != nil {
		_ = tr.Close()
		return err
	}
	defer func() { _ = n.Close() }()
	if err := n.Start(); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "%s listening on %s\n", n.Name(), n.Addr())
	if *join != "" {
		if err := n.Join(strings.Split(*join, ",")...); err != nil {
			return err
		}
	}
	go func() {
		for ev := range events {
			fmt.Fprintf(stderr, "event: %s %s (%s, inc %d)\n", ev.Type, ev.Member.Name, ev.Member.State, ev.Member.Incarnation)
		}
	}()
	fmt.Fprint(stdout, shellHelp)
	return shell(n, stdin, stdout)
}

func shell(n *swimring.Node, stdin io.Reader, stdout io.Writer) error {
	sc := bufio.NewScanner(stdin)
	for {
		fmt.Fprint(stdout, "> ")
		if !sc.Scan() {
			return sc.Err()
		}
		f := strings.Fields(sc.Text())
		if len(f) == 0 {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		done, err := execute(ctx, n, f, stdout)
		cancel()
		if err != nil {
			fmt.Fprintln(stdout, "error:", err)
		}
		if done {
			return nil
		}
	}
}

func execute(ctx context.Context, n *swimring.Node, f []string, stdout io.Writer) (bool, error) {
	need := func(k int) error {
		if len(f) != k {
			return fmt.Errorf("%s takes %d argument(s)", f[0], k-1)
		}
		return nil
	}
	switch f[0] {
	case "members":
		for _, m := range n.Membership.Members() {
			fmt.Fprintf(stdout, "  %-12s %-22s %-8s inc=%d\n", m.Name, m.Addr, m.State, m.Incarnation)
		}
	case "put":
		if len(f) < 3 {
			return false, fmt.Errorf("put takes a key and a value")
		}
		if err := n.Cache.Put(ctx, f[1], []byte(strings.Join(f[2:], " "))); err != nil {
			return false, err
		}
		fmt.Fprintln(stdout, "ok")
	case "get":
		if err := need(2); err != nil {
			return false, err
		}
		v, ok, err := n.Cache.Get(ctx, f[1])
		if err != nil {
			return false, err
		}
		if !ok {
			fmt.Fprintln(stdout, "(not found)")
			return false, nil
		}
		fmt.Fprintln(stdout, string(v))
	case "del":
		if err := need(2); err != nil {
			return false, err
		}
		if err := n.Cache.Delete(ctx, f[1]); err != nil {
			return false, err
		}
		fmt.Fprintln(stdout, "ok")
	case "owners":
		if err := need(2); err != nil {
			return false, err
		}
		fmt.Fprintln(stdout, strings.Join(n.Cache.Ring().LookupN(f[1], 3), " "))
	case "stats":
		fmt.Fprintf(stdout, "swim:  %+v\ncache: %+v\nlocal health: %d\n", n.Membership.Stats(), n.Cache.Stats(), n.Membership.LocalHealth())
	case "leave":
		if err := n.Membership.Leave(); err != nil {
			return true, err
		}
		time.Sleep(3 * n.Membership.Config().ProbeInterval)
		fmt.Fprintln(stdout, "left")
		return true, nil
	case "quit", "exit":
		return true, nil
	case "help":
		fmt.Fprint(stdout, shellHelp)
	default:
		return false, fmt.Errorf("unknown command %q (try help)", f[0])
	}
	return false, nil
}
