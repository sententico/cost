package csv

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/sententico/cost/internal/csv"
)

// settings (internal) structure for caching settings file
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
	sepSet      = ",\t|;:" // priority of separator runes automatically checked if none specified
	maxFieldLen = 256      // maximum field size allowed for Peek to qualify a separator
)

var commentSet = [...]string{"#", "//", "'"}

// atoi is a helper string-to-int function with selectable default value on error
func atoi(s string, d int) int {
	i, e := strconv.Atoi(s)
	if e != nil {
		return d
	}
	return i
}

// searchSig scans file type cache for CSV file specifier signature matching digest
//   CSV file type specifier syntax:
// 		"=<sep><cols>[,<col>[$<len>][:<pfx>[:<pfx>...]]]..."
//   examples:
// 		"=|35,7:INTL:DOM,12$13:20,21$3"
//		"=,120,102$16,17$3:Mon:Tue:Wed:Thu:Fri:Sat:Sun,62$5:S :M :L :XL"
func searchSig(dig Digest) (sig string) {
nextEntry:
	for sig = range config.cache {
		if !strings.HasPrefix(sig, "="+string(dig.Sep)) {
			continue nextEntry
		}
		for _, s := range dig.Split {
		nextTerm:
			for i, t := range strings.Split(sig[2:], ",") {
				v, c1, c2 := strings.Split(t, ":"), -1, -1
				switch cv := strings.Split(v[0], "$"); len(cv) {
				default:
					c2 = atoi(strings.Trim(cv[1], " "), -1)
					fallthrough
				case 1:
					c1 = atoi(strings.Trim(cv[0], " "), -1)
				}
				switch {
				case i == 0 && c1 != len(s):
					continue nextEntry
				case i == 0 || c1 <= 0:
					continue nextTerm
				case c1 > len(s) || c2 >= 0 && c2 != len(s[c1-1]):
					continue nextEntry
				case len(v) == 1:
					continue nextTerm
				}
				for _, pfx := range v[1:] {
					if strings.HasPrefix(s[c1-1], strings.TrimLeft(pfx, " ")) {
						continue nextTerm
					}
				}
				continue nextEntry
			}
		}
		return
	}
	return ""
}

// searchFSig scans file type cache for fixed-field file specifier signature matching digest
//   fixed-field TXT file type specifier syntax:
// 		"=({f|h}{<cols> | <col>:<pfx>[:<pfx>]...})..."
//   examples:
//		"=h80,h1:HEAD01,f132,f52:20,f126:S :M :L :XL"
//		"=f72,f72:T:F,f20:SKU"
func searchFSig(dig Digest) (sig string) {
nextEntry:
	for sig = range config.cache {
		if !strings.HasPrefix(sig, "=h") && !strings.HasPrefix(sig, "=f") {
			continue nextEntry
		}
		for i, p := range dig.Preview {
		nextTerm:
			for _, t := range strings.Split(sig[1:], ",") {
				v := strings.Split(strings.TrimLeft(t, " "), ":")
				c := atoi(strings.TrimLeft(strings.TrimRight(v[0], " "), "hf"), -1)
				switch {
				case v[0] == "" || c <= 0 || !strings.ContainsAny(v[0][0:1], "hf"):
					continue nextTerm
				case (v[0][0] == 'h' && i == 0 || v[0][0] == 'f' && i > 0) && len(v) == 1 && c != len(p):
					continue nextEntry
				case len(v) == 1 || v[0][0] == 'h' && i > 0 || v[0][0] == 'f' && i == 0:
					continue nextTerm
				}
				for _, pfx := range v[1:] {
					if strings.HasPrefix(p[c-1:], pfx) {
						continue nextTerm
					}
				}
				continue nextEntry
			}
		}
		return
	}
	return ""
}

// parseCMap parses a column-map string, returning the resulting map
//   CSV file type column-map syntax:
//		"(<cnam>[:<col>])..."
//   examples:
//		"name,age,income"
//		"name:1,age:4,income:13"
func parseCMap(cmap string) (m map[string]int) {
	if cmap != "" {
		m = make(map[string]int, 32)
		for i, t := range strings.Split(cmap, ",") {
			switch v := strings.Split(t, ":"); len(v) {
			case 1:
				if n := strings.Trim(v[0], " "); n != "" {
					m[n] = i + 1
				}
			default:
				if n, c := strings.Trim(v[0], " "), atoi(v[1], -1); n != "" && c > 0 {
					m[n] = c
				}
			}
		}
	}
	return
}

// parseFCMap parses a fixed-column-map string, returning the resulting map
//   fixed-field TXT file type column-map syntax:
//		"{{<cnam>|~}:<ecol> | <cnam>:<bcol>:<ecol>}...[<cnam>]"
//   examples:
//		"name:20,~:62,age:65,~:122,income"
//		"name:1:20,age:63:65,income:123:132"
func parseFCMap(fcmap string, wid int) (m map[string][2]int) {
	if fcmap == "" {
		return map[string][2]int{"~raw": {1, wid}}
	}
	m = make(map[string][2]int, 32)
	a, b, p, n := 0, 0, 0, ""
	for _, t := range strings.Split(fcmap, ",") {
		switch v := strings.Split(t, ":"); len(v) {
		case 1:
			if n, b = strings.Trim(v[0], " "), wid; n != "" && n != "~" && p < wid {
				m[n] = [2]int{p + 1, wid}
			}
		case 2:
			if n, b = strings.Trim(v[0], " "), atoi(v[1], -1); n != "" && n != "~" &&
				b > p && b <= wid {
				m[n] = [2]int{p + 1, b}
			}
		default:
			if n, a, b = strings.Trim(v[0], " "), atoi(v[1], -1), atoi(v[2], -1); n != "" && n != "~" &&
				a > 0 && b >= a && b <= wid {
				m[n] = [2]int{a, b}
			}
		}
		p = b
	}
	return
}

// handleSig is a goroutine helper that monitors the "sig" channel; when closed, "sigv" is
// modified
func handleSig(sig <-chan int, sigv *int) {
	go func() {
		for *sigv = range sig {
		}
		*sigv = -1
	}()
}

// readLn returns a channel into which a goroutine writes lines from file at "path" (channels
// also provided for errors and for the caller to signal a halt)
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
