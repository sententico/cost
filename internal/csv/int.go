package csv

import (
	"bufio"
	"fmt"
	"os"
)

type st4180 uint8

const (
	stSEP st4180 = iota
	stENCL
	stESC
	stUNENCL
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

// SliceCSV returns buffer with field slices for "csv" split by "sep", using a safe but tolerant
// implementation of RFC 4180
func SliceCSV(csv string, sep rune) ([]byte, []int) {
	buf, sl, st := make([]byte, 0, len(csv)), make([]int, 1, 4+len(csv)/4), stSEP
	for _, r := range csv {
		if r > '\x7e' || r != '\x09' && r < '\x20' {
			continue
		}
		switch st {
		case stSEP:
			switch r {
			case sep:
				sl = append(sl, len(buf))
			case '"':
				st = stENCL
			default:
				buf, st = append(buf, byte(r)), stUNENCL
			}
		case stENCL:
			switch r {
			case '"':
				st = stESC
			default:
				buf = append(buf, byte(r))
			}
		case stESC:
			switch r {
			case sep:
				sl, st = append(sl, len(buf)), stSEP
			default:
				buf, st = append(buf, byte(r)), stENCL
			}
		case stUNENCL:
			switch r {
			case sep:
				sl, st = append(sl, len(buf)), stSEP
			default:
				buf = append(buf, byte(r))
			}
		}
	}
	return buf, append(sl, len(buf))
}

// SplitCSV returns fields in "csv" split by "sep", using a safe but tolerant implementation of
// RFC 4180
func SplitCSV(csv string, sep rune) []string {
	buf, sl := SliceCSV(csv, sep)
	fields := make([]string, 0, len(sl))
	for i := 1; i < len(sl); i++ {
		fields = append(fields, string(buf[sl[i-1]:sl[i]]))
	}
	return fields
}
