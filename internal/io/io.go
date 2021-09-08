package io

import (
	"bufio"
	"fmt"
	"io"
	"os/user"
	"path/filepath"
	"strings"
)

// ReadLn returns a channel into which a goroutine writes text lines from an io.Reader (channels
// also provided for initial peek-ahead lines, errors and for the consumer to signal a halt)
func ReadLn(r io.Reader, peekLines int) (<-chan string, <-chan string, <-chan error, chan<- int) {
	peek, out, err, sig := make(chan string, peekLines), make(chan string, 64), make(chan error, 1), make(chan int)
	go func() {
		defer func() {
			if e := recover(); e != nil {
				err <- e.(error)
			}
			close(err)
			close(out)
		}()
		ln := bufio.NewScanner(r) // bufio.ReadString('\n') may handle EOF better with io.Pipe input

		for len(out) < cap(peek) && len(out) < cap(out) && ln.Scan() {
			peek <- ln.Text()
			out <- ln.Text() // assumes out not processed until peek closed
		}
		for close(peek); ln.Scan(); {
			select {
			case out <- ln.Text():
			case <-sig:
				return
			}
		}
		if e := ln.Err(); e != nil {
			panic(fmt.Errorf("error scanning lines (%v)", e))
		}
	}()
	return peek, out, err, sig
}

// ResolveName is a helper function that resolves resource names (pathnames, ...)
func ResolveName(n string) string {
	if strings.HasPrefix(n, "~/") {
		if u, e := user.Current(); e == nil {
			if rn, e := filepath.Abs(u.HomeDir + n[1:]); e == nil {
				return rn
			}
		}
	} else if rn, e := filepath.Abs(n); e == nil {
		return rn
	}
	return n
}
