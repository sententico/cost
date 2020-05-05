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

// SliceCSV returns buffer with field slices for "csv" split by "sep", approximating RFC 4180
func SliceCSV(csv string, sep rune) ([]byte, []int) {
	buf, sl, encl := make([]byte, 0, len(csv)), make([]int, 1, len(csv)+2), false
	for _, r := range csv {
		switch {
		case r > '\x7e' || r != '\x09' && r < '\x20':
			// alternatively replace non-printable ASCII runes with a blank: buf = append(buf, ' ')
		case r == '"':
			encl = !encl
		case !encl && r == sep:
			sl = append(sl, len(buf))
		default:
			buf = append(buf, byte(r))
		}
	}
	return buf, append(sl, len(buf))
}

// SplitCSV returns fields in "csv" split by "sep", approximating RFC 4180
func SplitCSV(csv string, sep rune) []string {
	buf, sl := SliceCSV(csv, sep)
	fields := make([]string, 0, len(sl))
	for i := 1; i < len(sl); i++ {
		fields = append(fields, string(buf[sl[i-1]:sl[i]]))
	}
	return fields
}
