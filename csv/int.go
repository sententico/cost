package csv

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/sententico/cost/internal/csv"
)

// settings structure (internal) ...
type settings struct {
	read  bool
	path  string
	cache Settings
}

var (
	config settings
	_int   csv.Placeholder
)

const (
	previewRows = 6        // number of preview rows returned by Peek
	sepSet      = ",\t|;:" // order of separator runes automatically checked if none specified
	maxFieldLen = 256      // maximum field size allowed for Peek to qualify a separator
)

var commentSet = [...]string{"#", "//", "'"}

// parseCMap ...
func parseCMap(cmap string) (m map[string]int) {
	if cmap != "" {
		m = make(map[string]int, 32)
		for i, t := range strings.Split(cmap, ",") {
			switch v := strings.Split(t, ":"); len(v) {
			case 1:
				m[strings.Trim(v[0], " ")] = i + 1
			default:
				if c, _ := strconv.Atoi(v[1]); c > 0 {
					m[strings.Trim(v[0], " ")] = c
				}
			}
		}
	}
	return
}

// parseFCMap ...
func parseFCMap(fcmap string, wid int) (m map[string][2]int) {
	if fcmap == "" {
		return map[string][2]int{"~raw": {1, wid}}
	}
	m = make(map[string][2]int, 32)
	a, b, p := 0, 0, 0
	for _, t := range strings.Split(fcmap, ",") {
		switch v := strings.Split(t, ":"); len(v) {
		case 1:
			if p < wid {
				m[strings.Trim(v[0], " ")] = [2]int{p + 1, wid}
			}
			continue
		case 2:
			if b, _ = strconv.Atoi(v[1]); b > p && b <= wid {
				m[strings.Trim(v[0], " ")] = [2]int{p + 1, b}
			}
		default:
			a, _ = strconv.Atoi(v[1])
			b, _ = strconv.Atoi(v[2])
			if a > 0 && b >= a && b <= wid {
				m[strings.Trim(v[0], " ")] = [2]int{a, b}
			}
		}
		p = b
	}
	return
}

// handleSig is a goroutine that monitors the "sig" channel; when closed, "sigv" is modified
func handleSig(sig <-chan int, sigv *int) {
	go func() {
		for *sigv = range sig {
		}
		*sigv = -1
	}()
}

// readLn returns a channel into which a goroutine writes lines from file at "path" (channels
// also provided for errors and for the caller to signal a halt).
func readLn(path string) (<-chan string, <-chan error, chan<- int) {
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
		handleSig(sig, &sigv)

		ln := bufio.NewScanner(file)
		for ; sigv == 0 && ln.Scan(); out <- ln.Text() {
		}
		if e := ln.Err(); e != nil {
			panic(fmt.Errorf("problem reading %q (%v)", path, e))
		}
	}()
	return out, err, sig
}

// splitCSV returns a slice of fields in "csv" split by "sep", approximately following RFC 4180
func splitCSV(csv string, sep rune) (fields []string) {
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
