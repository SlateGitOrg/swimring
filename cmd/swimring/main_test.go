package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestUsage(t *testing.T) {
	var out, errOut bytes.Buffer
	if err := run(nil, nil, &out, &errOut); !errors.Is(err, errUsage) {
		t.Fatalf("err = %v", err)
	}
	if err := run([]string{"bogus"}, nil, &out, &errOut); !errors.Is(err, errUsage) {
		t.Fatalf("err = %v", err)
	}
	if err := run([]string{"help"}, nil, &out, &errOut); err != nil || !strings.Contains(out.String(), "usage") {
		t.Fatalf("help: %v %q", err, out.String())
	}
	if err := run([]string{"sim", "-n", "2", "-crash", "2"}, nil, &out, &errOut); err == nil {
		t.Fatal("invalid sim flags accepted")
	}
	if err := run([]string{"node"}, nil, &out, &errOut); !errors.Is(err, errUsage) {
		t.Fatalf("node without name: %v", err)
	}
}

func TestSimCommand(t *testing.T) {
	var out, errOut bytes.Buffer
	args := []string{"sim", "-n", "8", "-loss", "0.05", "-crash", "1", "-slow", "1", "-duration", "30s", "-seed", "3"}
	if err := run(args, nil, &out, &errOut); err != nil {
		t.Fatalf("%v: %s", err, errOut.String())
	}
	s := out.String()
	for _, want := range []string{"bootstrapped 8 members", "crashed n007", "n007   dead", "sees 7 live members"} {
		if !strings.Contains(s, want) {
			t.Fatalf("output missing %q:\n%s", want, s)
		}
	}
}

func TestDemoSimulatedPart(t *testing.T) {
	var out, errOut bytes.Buffer
	if err := run([]string{"demo", "-skip-udp"}, nil, &out, &errOut); err != nil {
		t.Fatalf("%v: %s", err, errOut.String())
	}
	for _, want := range []string{"60/60 keys readable", "60/60 back at 3 live replicas", "re-merged all 7 survivors"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("demo output missing %q:\n%s", want, out.String())
		}
	}
}

func TestEvalQuick(t *testing.T) {
	var out, errOut bytes.Buffer
	file := filepath.Join(t.TempDir(), "sub", "EVAL.md")
	if err := run([]string{"eval", "-quick", "-out", file}, nil, &out, &errOut); err != nil {
		t.Fatalf("%v: %s", err, errOut.String())
	}
	b, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, out.Bytes()) || !strings.Contains(string(b), "## 5. Ring balance") {
		t.Fatal("report file does not match stdout")
	}
}

func TestNodeShell(t *testing.T) {
	script := strings.Join([]string{
		"", "help", "members", "put color deep blue", "get color", "owners color", "stats",
		"del color", "get color", "get", "put x", "bogus", "quit",
	}, "\n") + "\n"
	var out, errOut bytes.Buffer
	if err := run([]string{"node", "-name", "solo", "-bind", "127.0.0.1:0"}, strings.NewReader(script), &out, &errOut); err != nil {
		t.Fatalf("%v: %s", err, errOut.String())
	}
	s := out.String()
	for _, want := range []string{"solo listening on 127.0.0.1:", "deep blue", "(not found)", "unknown command", "get takes 1 argument", "put takes a key and a value", "local health: 0"} {
		if !strings.Contains(s, want) {
			t.Fatalf("shell output missing %q:\n%s", want, s)
		}
	}
}

func TestNodeLeave(t *testing.T) {
	var out, errOut bytes.Buffer
	if err := run([]string{"node", "-name", "leaver", "-bind", "127.0.0.1:0", "-join", "127.0.0.1:1", "-interval", "50ms"}, strings.NewReader("leave\n"), &out, &errOut); err != nil {
		t.Fatalf("%v: %s", err, errOut.String())
	}
	if !strings.Contains(out.String(), "left") {
		t.Fatalf("output: %s", out.String())
	}
}
