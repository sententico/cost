package csv

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sententico/cost/internal/csv"
)

// Digest structure summarizing a CSV/TXT file for identification
type Digest struct {
	Preview  []string      // preview rows (excluding non-blank non-comment lines)
	Rows     int           // estimated total file rows
	Comment  string        // inferred comment line prefix
	Sep      rune          // inferred field separator rune (if CSV)
	Split    [][]string    // trimmed fields of preview rows split by "sep" (if CSV)
	Heading  bool          // first row probable heading (if CSV)
	Sig      string        // file-type signature (specifier or heading MD5 hash, if determined)
	Settings SettingsEntry // file-type settings from settings file under signature (if found)
}

// SettingsEntry contains information for a CSV file-type cached from the settings file under
// its signature (specifier or heading MD5 hash)
type SettingsEntry struct {
	Cols string    // column map
	Type string    // file-type ID
	Ver  string    // file version identifier
	Date time.Time // entry update timestamp
	Lock bool      // entry locked to automatic updates
}

// settingsCache for settings file mapping file-type info by signature (specifier or heading
// MD5 hash)
type settingsCache struct {
	read  bool
	path  string
	mutex sync.Mutex
	cache map[string]SettingsEntry
}

// Settings global holds setting cache from settings file
var Settings *settingsCache = &settingsCache{}

// Cache method on SettingsCache reads JSON file-type settings file into cache
func (settings *settingsCache) Cache(path string) (err error) {
	settings.mutex.Lock()
	defer settings.mutex.Unlock()
	var b []byte
	settings.path, settings.read = path, false
	if b, err = ioutil.ReadFile(path); err == nil {
		err = json.Unmarshal(b, &settings.cache)
		settings.read = err == nil
	}
	if !settings.read {
		settings.cache = make(map[string]SettingsEntry)
	}
	return
}

// Write method on SettingsCache writes (potentially modified) cached file-type settings back to
// the JSON settings file
func (settings *settingsCache) Write() error {
	settings.mutex.Lock()
	defer settings.mutex.Unlock()

	b, err := json.MarshalIndent(settings.cache, "", "    ")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(settings.path, b, 0644)
}

// Get method on SettingsCache returns cache entry under file-type signature
func (settings *settingsCache) Get(sig string) SettingsEntry {
	settings.mutex.Lock()
	defer settings.mutex.Unlock()
	return settings.cache[sig]
}

// GetSpecs method on SettingsCache returns list of file-type specifications in the settings cache
func (settings *settingsCache) GetSpecs() (specs []string) {
	settings.mutex.Lock()
	defer settings.mutex.Unlock()
	for sig := range settings.cache {
		if strings.HasPrefix(sig, "=") && len(sig) > 2 {
			specs = append(specs, sig)
		}
	}
	return
}

// Set method on SettingsCache returns entry set in settings cache under file-type signature
func (settings *settingsCache) Set(sig string, entry SettingsEntry) SettingsEntry {
	settings.mutex.Lock()
	defer settings.mutex.Unlock()
	settings.cache[sig] = entry
	return entry
}

// Peek returns a digest to identify the CSV (or fixed-field TXT file) at "path". This digest
// consists of a preview slice of raw data rows (without blank or comment lines), a total file row
// estimate, the comment prefix used (if any), and if a CSV, the field separator, trimmed fields
// of the preview data rows split by it, and a hint whether to treat this row as a heading. If
// determined, a file-type signature (specifier or heading MD5 hash) with any cached info mapped
// to it are provided.
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
nextLine:
	for row < previewRows && bf.Scan() {
		switch ln := bf.Text(); {
		case len(strings.TrimLeft(ln, " ")) == 0:
		case dig.Comment != "" && strings.HasPrefix(ln, dig.Comment):
		case row < 0:
			row = 0
			for _, p := range commentSet {
				if strings.HasPrefix(ln, p) {
					dig.Comment = p
					continue nextLine
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
nextSep:
	for _, r := range sepSet {
		c, sl := 0, []string{}
		for _, ln := range dig.Preview {
			if sl = csv.SplitCSV(ln, r); len(sl) <= max || len(sl) != c && c > 0 {
				continue nextSep
			}
			for _, f := range sl {
				if len(f) > maxFieldLen {
					continue nextSep
				}
			}
			c = len(sl)
		}
		max, dig.Sep = c, r
	}
	if dig.Sep != '\x00' {
		for rc, r := range dig.Preview {
			dig.Split = append(dig.Split, []string{})
			for _, f := range csv.SplitCSV(r, dig.Sep) {
				dig.Split[rc] = append(dig.Split[rc], strings.Trim(f, " "))
			}
		}
		qh := make(map[string]int, max)
		for _, h := range dig.Split[0] {
			if _, e := strconv.ParseFloat(h, 64); e != nil && len(h) > 0 {
				qh[h]++
			}
		}
		dig.Heading = len(qh) == max
	}
	switch {
	case dig.Heading:
		dig.Sig = fmt.Sprintf("%x", md5.Sum([]byte(strings.Join(dig.Split[0], string(dig.Sep)))))
	case dig.Sep == '\x00':
		getFSpec(&dig)
	default:
		getSpec(&dig)
	}
	dig.Settings = Settings.Get(dig.Sig)
	return
}

// ReadFixed returns a channel into which a goroutine writes maps of fixed-field TXT rows from
// file at "path" keyed by "fcols" (channels also provided for errors and for the caller to signal
// a halt).  Fields selected by column ranges in "fcols" map are trimmed of blanks; empty fields
// are suppressed; "head" lines, blank lines and those prefixed by "comment" are skipped.
func ReadFixed(path, fcols, comment string, head bool) (<-chan map[string]string, <-chan error, chan<- int) {
	out, err, sig, sigv := make(chan map[string]string, 64), make(chan error, 1), make(chan int), 0
	go func() {
		defer func() {
			if e := recover(); e != nil {
				err <- e.(error)
			}
			close(err)
			close(out)
		}()
		in, ierr, isig := csv.ReadLn(path)
		defer close(isig)
		csv.HandleSig(sig, &sigv)

		cols, wid, line, algn := map[string][2]int{}, 0, 0, 0
		for ln := range in {
			for line++; ; {
				switch {
				case len(strings.TrimLeft(ln, " ")) == 0:
				case comment != "" && strings.HasPrefix(ln, comment):
				case head:
					head = false
				case wid == 0:
					wid = len(ln)
					if cols = parseFCMap(fcols, wid); len(cols) == 0 {
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

// Read returns a channel into which a goroutine writes field maps of CSV rows from file at "path"
// keyed by "cols" column selector map, or if "" and "head" present, by the heading in the first
// data row (channels also provided for errors and for the caller to signal a halt). CSV separator
// is "sep", or if \x00, will be inferred. Fields are trimmed of blanks and double-quotes (which
// may enclose separators); empty fields are suppressed; blank lines and those prefixed by
// "comment" are skipped.
func Read(path, cols, comment string, head bool, sep rune) (<-chan map[string]string, <-chan error, chan<- int) {
	out, err, sig, sigv := make(chan map[string]string, 64), make(chan error, 1), make(chan int), 0
	go func() {
		defer func() {
			if e := recover(); e != nil {
				err <- e.(error)
			}
			close(err)
			close(out)
		}()
		in, ierr, isig := csv.ReadLn(path)
		defer close(isig)
		csv.HandleSig(sig, &sigv)

		vcols, wid, line, algn := make(map[string]int, 32), 0, 0, 0
		for ln := range in {
			for line++; ; {
				switch {
				case len(strings.TrimLeft(ln, " ")) == 0:
				case comment != "" && strings.HasPrefix(ln, comment):
				case sep == '\x00':
					for _, r := range sepSet {
						if c := len(csv.SplitCSV(ln, r)); c > wid {
							wid, sep = c, r
						}
					}
					continue
				case len(vcols) == 0:
					sl, uc, sc, mc := csv.SplitCSV(ln, sep), make(map[int]int), make(map[string]int), 0
					for c, i := range parseCMap(cols) {
						if c = strings.Trim(c, " "); c != "" && i > 0 {
							sc[c] = i
							if uc[i]++; i > mc {
								mc = i
							}
						}
					}
					for i, c := range sl {
						if c = strings.Trim(c, " "); c != "" && (len(sc) == 0 || sc[c] > 0) {
							vcols[c] = i + 1
						}
					}
					switch wid = len(sl); {
					case len(sc) == 0 && (!head || len(vcols) != wid):
						panic(fmt.Errorf("no heading in CSV file %q and no column map provided", path))
					case len(sc) == 0:
					case len(vcols) == len(sc):
					case len(vcols) > 0:
						panic(fmt.Errorf("missing columns in CSV file %q", path))
					case head || mc > wid:
						panic(fmt.Errorf("column map incompatible with CSV file %q", path))
					case len(uc) < len(sc):
						panic(fmt.Errorf("ambiguous column map provided for CSV file %q", path))
					default:
						vcols = sc
						continue
					}

				default:
					if b, sl := csv.SliceCSV(ln, sep); len(sl)-1 == wid {
						m, f := make(map[string]string, len(vcols)), ""
						head = true
						for c, i := range vcols {
							if sl[i-1] == sl[i] {
								head = false
							} else if bs := bytes.Trim(b[sl[i-1]:sl[i]], " "); len(bs) > 0 {
								f = string(bs)
								m[c], head = f, head && f == c
							} else {
								head = false
							}
						}
						if !head && len(m) > 0 {
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
