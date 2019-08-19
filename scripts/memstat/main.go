package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const refreshInterval = 100 * time.Millisecond

func main() {
	if os.Args[1] == "--help" || os.Args[1] == "-h" {
		_, _ = fmt.Fprintf(os.Stderr, `memstat: 

CLI tool for detecting peak (RSS) memory usage for complex CLI applicaions (e.g go test). It supports gathering total peak RSS from 
all child processes forked by monitored process on Linux OS.

Granularity of detection can be controled via 'refreshInterval' constant.

Usage: 

go run main.go <command to execute &  monitor>
go run main.go go test ./... -v

NOTE: It is basic CLI, it does not support proper signal passing etc
`)
		os.Exit(1)
	}

	if runtime.GOOS != "linux" {
		_, _ = fmt.Fprintf(os.Stderr, "memstat: only linux OS supported. Detected %s", runtime.GOOS)
		os.Exit(1)
	}

	cmd := exec.Command(os.Args[1], os.Args[2:]...)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		// For linux only, kill this if the go test process dies before the cleanup.
		Pdeathsig: syscall.SIGKILL,
	}
	cmd.Env = os.Environ()
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin

	// Check peak memory.
	pid := os.Getpid()
	stats := &statsTree{children: map[int]*statsTree{}}
	totalPeakRSS := int64(0)

	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	go func() {
		for {
			select {
			case <-ctx.Done():
				close(ready)
				return
			case <-time.After(refreshInterval):
			}
			totalRSS := int64(0)
			updateChildrenStatsTree(ctx, pid, stats, &totalRSS)
			if totalRSS > totalPeakRSS {
				totalPeakRSS = totalRSS
			}
		}
	}()

	if err := cmd.Start(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "memstat: failed to start passed command: %v", err)
		os.Exit(1)
	}
	err := cmd.Wait()

	cancel()
	<-ready

	_, _ = fmt.Fprintln(os.Stdout, "[memstat] peak RSS summary for all children")
	for _, ch := range stats.children {
		ch.Print(os.Stdout, "")
	}
	_, _ = fmt.Fprintf(os.Stdout, "TotalPeak RSS: %v Bytes\n", totalPeakRSS)

	if exitErr, ok := err.(*exec.ExitError); ok {
		// The program has exited with an exit code not 0.
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
			os.Exit(status.ExitStatus())
		}
	}
	os.Exit(0)
}

type statsTree struct {
	pid          int
	name         string
	peakRSSBytes int64
	children     map[int]*statsTree
}

func (s statsTree) Print(w io.Writer, pad string) {
	_, _ = fmt.Fprintf(w, fmt.Sprintf("%spid: %d name: %s peakRSS: %v\n", pad, s.pid, s.name, s.peakRSSBytes))
	for _, ch := range s.children {
		ch.Print(w, pad+"--")
	}
}

func updateChildrenStatsTree(ctx context.Context, pid int, root *statsTree, totalRSS *int64) {
	children, err := pgrepWithContext(ctx, pid)
	if err != nil {
		panic(fmt.Sprintf("Failed to get children using pgrep for pid %d; err: %v", pid, err))
	}
	for _, child := range children {
		if child == 0 {
			continue
		}

		if _, ok := root.children[child]; !ok {
			root.children[child] = &statsTree{pid: child, children: map[int]*statsTree{}}
		}
		s := root.children[child]

		b, err := ioutil.ReadFile(fmt.Sprintf("/proc/%d/stat", child))
		if err != nil {
			// Process might not exists anymore.
			return
		}

		// https://linux.die.net/man/5/proc
		stat := strings.Split(string(b), " ")

		if s.pid == 0 {
			s.pid = child
			s.name = stat[1]
		}

		rss, err := strconv.ParseInt(stat[23], 10, 64)
		if err != nil {
			panic(fmt.Sprintf("Failed to parse rss %s from /proc/%d/stat; err: %v", stat[23], child, err))
		}
		if rss > s.peakRSSBytes {
			s.peakRSSBytes = rss
		}
		*totalRSS += rss
		updateChildrenStatsTree(ctx, child, s, totalRSS)
	}
}

func pgrepWithContext(ctx context.Context, pid int) ([]int, error) {
	pgrep, err := exec.LookPath("pgrep")
	if err != nil {
		return nil, err
	}
	out, err := exec.CommandContext(ctx, pgrep, "-P", strconv.Itoa(pid)).CombinedOutput()
	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			// The program has exited with an exit code not 0, assume no error. pgrep ends with 1 if no process found (our process does not have any children)
			return nil, nil
		}
		return nil, err
	}
	lines := strings.Split(string(out), "\n")
	ret := make([]int, 0, len(lines))
	for _, l := range lines {
		if len(l) == 0 {
			continue
		}
		i, err := strconv.Atoi(l)
		if err != nil {
			continue
		}
		ret = append(ret, i)
	}
	return ret, nil
}
