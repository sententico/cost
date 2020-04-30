package csv

import (
	"bufio"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/sententico/cost/internal/csv"
)

// Digest structure summarizing a CSV/TXT file for identification
type Digest struct {
	Preview []string   // preview rows (excluding non-blank non-comment lines)
	Rows    int        // estimated total file rows
	Comment string     // inferred comment line prefix
	Sep     rune       // inferred field separator rune (if CSV)
	Split   [][]string // trimmed fields of preview rows split by "sep" (if CSV)
	Heading bool       // first row probable heading (if CSV)
	MD5     string     // hash of heading (if CSV with heading)
	Cache   CacheEntry // matching file type config in settings file (if CSV with heading)
}

// CacheEntry structure ...
type CacheEntry struct {
	Cols string    // column map
	Type string    // file type
	Ver  string    // file version identifier
	Date time.Time // entry update timestamp
	Lock bool      // entry locked to automatic updates
}

// Settings structure (external) ...
type Settings map[string]CacheEntry

var (
	_ext csv.Placeholder
)

// GetConfig returns a reference to the config read from settings file at "path"
func GetConfig(path string) Settings {
	config.path = path
	if b, err := ioutil.ReadFile(path); err == nil {
		config.read = json.Unmarshal(b, &config.cache) == nil
	}
	if config.cache == nil {
		config.cache = make(Settings)
	}
	return config.cache
}

// SetConfig writes config back to settings file
func SetConfig() error {
	b, err := json.MarshalIndent(config.cache, "", "    ")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(config.path, b, 0644)
}

// Peek returns a digest to identify the CSV (or TXT file) at "path". This digest consists of a
// preview slice of raw data rows (without blank or comment lines), a total file row estimate, the
// comment prefix used (if any), and if a CSV, the field separator, trimmed fields of the first
// data row split by it, a hint whether to treat this row as a heading, and a hash if a heading.
func Peek(path string) (dig Digest, err error) {
	defer func() {
		if e := recover(); e != nil {
			err = e.(error)
		}
	}()
	file, e := os.Open(path)
	if e != nil {
		panic(fmt.Errorf("can't access %q (%v)", path, e))
	}
	defer file.Close()
	info, e := file.Stat()
	if e != nil {
		panic(fmt.Errorf("can't access %q metadata (%v)", path, e))
	}
	bf, row, tlen, max := bufio.NewScanner(file), -1, 0, 1
getLine:
	for row < previewRows && bf.Scan() {
		switch ln := bf.Text(); {
		case len(strings.TrimLeft(ln, " ")) == 0:
		case dig.Comment != "" && strings.HasPrefix(ln, dig.Comment):
		case row < 0:
			row = 0
			for _, p := range commentSet {
				if strings.HasPrefix(ln, p) {
					dig.Comment = p
					continue getLine
				}
			}
			fallthrough
		default:
			row++
			tlen += len(ln)
			dig.Preview = append(dig.Preview, ln)
		}
	}
	switch e := bf.Err(); {
	case e != nil:
		panic(fmt.Errorf("problem reading %q (%v)", path, e))
	case row < 1:
		panic(fmt.Errorf("%q does not contain data", path))
	case row < previewRows:
		dig.Rows = row
	default:
		dig.Rows = int(float64(info.Size())/float64(tlen-len(dig.Preview[0])+row-1)*0.995+0.5) * (row - 1)
	}
getSep:
	for _, r := range sepSet {
		c, sl := 0, []string{}
		for _, ln := range dig.Preview {
			if sl = splitCSV(ln, r); len(sl) <= max || len(sl) != c && c > 0 {
				continue getSep
			}
			for _, f := range sl {
				if len(f) > maxFieldLen {
					continue getSep
				}
			}
			c = len(sl)
		}
		max, dig.Sep = c, r
	}
	if dig.Sep != '\x00' {
		for rc, r := range dig.Preview {
			dig.Split = append(dig.Split, []string{})
			for _, f := range splitCSV(r, dig.Sep) {
				dig.Split[rc] = append(dig.Split[rc], strings.Trim(f, " "))
			}
		}
		qh := make(map[string]int, max)
		for _, h := range dig.Split[0] {
			if _, e := strconv.ParseFloat(h, 64); e != nil && len(h) > 0 {
				qh[h]++
			}
		}
		if dig.Heading = len(qh) == max; dig.Heading {
			dig.MD5 = fmt.Sprintf("%x", md5.Sum([]byte(strings.Join(dig.Split[0], string(dig.Sep)))))
			dig.Cache = config.cache[dig.MD5]
		}
	}
	return
}

// ReadFixed returns a channel into which a goroutine writes maps of fixed-field TXT rows from file
// at "path" keyed by "cols" (channels also provided for errors and for the caller to signal a
// halt).  Fields selected by byte ranges in the "cols" map are trimmed of blanks; empty fields
// are suppressed; blank lines and those prefixed by "comment" are skipped.
func ReadFixed(path, fcmap, comment string) (<-chan map[string]string, <-chan error, chan<- int) {
	out, err, sig, sigv := make(chan map[string]string, 64), make(chan error, 1), make(chan int), 0
	go func() {
		defer func() {
			if e := recover(); e != nil {
				err <- e.(error)
			}
			close(err)
			close(out)
		}()
		in, ierr, isig := readLn(path)
		defer close(isig)
		handleSig(sig, &sigv)

		cols, wid, line, algn := map[string][2]int{}, 0, 0, 0
		for ln := range in {
			for line++; ; {
				switch {
				case len(strings.TrimLeft(ln, " ")) == 0:
				case comment != "" && strings.HasPrefix(ln, comment):
				case wid == 0:
					wid = len(ln)
					if cols = parseFCMap(fcmap, wid); len(cols) == 0 {
						panic(fmt.Errorf("bad column map provided for fixed-field file %q", path))
					}
					continue

				case len(ln) != wid:
					if algn++; line > 200 && float64(algn)/float64(line) > 0.02 {
						panic(fmt.Errorf("excessive column misalignment in fixed-field file %q (>%d rows)", path, algn))
					}
				default:
					m := make(map[string]string, len(cols))
					for c, r := range cols {
						if f := strings.Trim(ln[r[0]-1:r[1]], " "); len(f) > 0 {
							m[c] = f
						}
					}
					if len(m) > 0 {
						m["~line"] = strconv.Itoa(line)
						out <- m
					}
				}
				break
			}
			if sigv != 0 {
				return
			}
		}
		if e := <-ierr; e != nil {
			panic(fmt.Errorf("problem reading fixed-field file %q (%v)", path, e))
		}
	}()
	return out, err, sig
}

// Read returns a channel into which a goroutine writes field maps of CSV rows from file at
// "path" keyed by "cols" map which also identifies select columns for extraction, or if nil, by
// the heading in the first data row (channels also provided for errors and for the caller to
// signal a halt).  CSV separator is "sep", or if \x00, will be inferred.  Fields are trimmed of
// blanks and double-quotes (which may enclose separators); empty fields are suppressed; blank
// lines and those prefixed by "comment" are skipped.
func Read(path, cmap, comment string, sep rune) (<-chan map[string]string, <-chan error, chan<- int) {
	out, err, sig, sigv := make(chan map[string]string, 64), make(chan error, 1), make(chan int), 0
	go func() {
		defer func() {
			if e := recover(); e != nil {
				err <- e.(error)
			}
			close(err)
			close(out)
		}()
		in, ierr, isig := readLn(path)
		defer close(isig)
		handleSig(sig, &sigv)

		vcols, wid, line, algn := make(map[string]int, 32), 0, 0, 0
		for ln := range in {
			for line++; ; {
				switch {
				case len(strings.TrimLeft(ln, " ")) == 0:
				case comment != "" && strings.HasPrefix(ln, comment):
				case sep == '\x00':
					for _, r := range sepSet {
						if c := len(splitCSV(ln, r)); c > wid {
							wid, sep = c, r
						}
					}
					continue
				case len(vcols) == 0:
					sl, uc, sc, mc, qc := splitCSV(ln, sep), make(map[int]int), make(map[string]int), 0, make(map[string]int)
					for c, i := range parseCMap(cmap) {
						if c = strings.Trim(c, " "); c != "" && i > 0 {
							sc[c] = i
							if uc[i]++; i > mc {
								mc = i
							}
						}
					}
					for i, c := range sl {
						if c = strings.Trim(c, " "); c != "" {
							if len(sc) == 0 || sc[c] > 0 {
								vcols[c] = i + 1
							}
							if _, e := strconv.ParseFloat(c, 64); e != nil {
								qc[c] = i + 1
							}
						}
					}
					switch wid = len(sl); {
					case len(sc) == 0 && len(qc) == wid:
					case len(sc) == 0:
						panic(fmt.Errorf("no heading in CSV file %q and no column map provided", path))
					case len(vcols) == len(sc):
					case len(vcols) > 0:
						panic(fmt.Errorf("missing columns in CSV file %q", path))
					case len(qc) == wid || mc > wid:
						panic(fmt.Errorf("column map incompatible with CSV file %q", path))
					case len(uc) < len(sc):
						panic(fmt.Errorf("ambiguous column map provided for CSV file %q", path))
					default:
						vcols = sc
						continue
					}

				default:
					if sl := splitCSV(ln, sep); len(sl) == wid {
						m, heading := make(map[string]string, len(vcols)), true
						for c, i := range vcols {
							f := strings.Trim(sl[i-1], " ")
							if len(f) > 0 {
								m[c] = f
							}
							heading = heading && f == c
						}
						if !heading && len(m) > 0 {
							m["~line"] = strconv.Itoa(line)
							out <- m
						}
					} else if algn++; line > 200 && float64(algn)/float64(line) > 0.02 {
						panic(fmt.Errorf("excessive column misalignment in CSV file %q (>%d rows)", path, algn))
					}
				}
				break
			}
			if sigv != 0 {
				return
			}
		}
		if e := <-ierr; e != nil {
			panic(fmt.Errorf("problem reading CSV file %q (%v)", path, e))
		}
	}()
	return out, err, sig
}
