package csv

import (
	"bufio"
	"fmt"
	"os"
)

// HandleSig is a goroutine helper that monitors the "sig" channel; when closed, "sigv" is
// modified
func HandleSig(sig <-chan int, sigv *int) {
	go func() {
		for *sigv = range sig {
		}
		*sigv = -1
	}()
}

// ReadLn returns a channel into which a goroutine writes lines from file at "path" (channels
// also provided for errors and for the caller to signal a halt)
func ReadLn(path string) (<-chan string, <-chan error, chan<- int) {
	out, err, sig, sigv := make(chan string, 64), make(chan error, 1), make(chan int), 0
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
		HandleSig(sig, &sigv)

		ln := bufio.NewScanner(file)
		for ; sigv == 0 && ln.Scan(); out <- ln.Text() {
		}
		if e := ln.Err(); e != nil {
			panic(fmt.Errorf("problem reading %q (%v)", path, e))
		}
	}()
	return out, err, sig
}

// SplitCSV returns a slice of fields in "csv" split by "sep", approximately following RFC 4180
func SplitCSV(csv string, sep rune) (fields []string) {
	field, encl := "", false
	for _, r := range csv {
		switch {
		case r > '\x7e' || r != '\x09' && r < '\x20':
			// alternatively replace non-printables with a blank: field += " "
		case r == '"':
			encl = !encl
		case !encl && r == sep:
			fields = append(fields, field)
			field = ""
		default:
			field += string(r)
		}
	}
	return append(fields, field)
}
