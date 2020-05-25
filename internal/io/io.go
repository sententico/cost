package io

import (
	"bufio"
	"fmt"
	"os"
)

// ReadLn returns a channel into which a goroutine writes text lines from file at "path" (channels
// also provided for errors and for the consumer to signal a halt)
func ReadLn(path string) (<-chan string, <-chan error, chan<- int) {
	out, err, sig := make(chan string, 64), make(chan error, 1), make(chan int)
	go func() {
		defer func() {
			if e := recover(); e != nil {
				err <- e.(error)
			}
			close(err)
			close(out)
		}()
		file, e := os.Open(path)
		if e != nil {
			panic(fmt.Errorf("can't access %q (%v)", path, e))
		}
		defer file.Close()

		ln := bufio.NewScanner(file)
		for ln.Scan() {
			select {
			case out <- ln.Text():
			case <-sig:
				return
			}
		}
		if e := ln.Err(); e != nil {
			panic(fmt.Errorf("problem reading %q (%v)", path, e))
		}
	}()
	return out, err, sig
}
