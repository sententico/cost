package csv

import (
	"bufio"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/sententico/cost/internal/io"
)

// Digest structure summarizing a CSV/TXT file for identification
type Digest struct {
	Preview  []string      // preview rows (excluding non-blank non-comment lines)
	Rows     int           // estimated total file rows
	Comment  string        // inferred comment line prefix
	Sep      rune          // inferred field separator rune (if CSV)
	Split    [][]string    // trimmed fields of preview rows split by "sep" (if CSV)
	Heading  bool          // first row inferred as heading
	Sig      string        // file-type signature (specifier or heading MD5 hash, if determined)
	Settings SettingsEntry // file-type settings from settings file under signature (if found)
}

// SettingsEntry contains information for a CSV file-type cached from the settings file under
// its signature (specifier or heading MD5 hash)
type SettingsEntry struct {
	Cols string    // column map
	Type string    // file-type identifier
	Ver  string    // file version
	Date time.Time // entry update timestamp
	Lock bool      // entry locked to automatic updates
}

// settingsCache for settings file mapping file-type info by signature (specifier or heading
// MD5 hash)
type settingsCache struct {
	writable bool
	path     string
	mutex    sync.Mutex
	cache    map[string]SettingsEntry
}

// Settings global holds setting cache from settings file
var Settings *settingsCache = &settingsCache{}

// Cache method on SettingsCache reads JSON file-type settings file into cache
func (settings *settingsCache) Cache(path string) (err error) {
	settings.mutex.Lock()
	defer settings.mutex.Unlock()
	var b []byte

	if strings.HasPrefix(path, "~/") {
		var u *user.User
		if u, err = user.Current(); err != nil {
			return
		}
		if settings.path, err = filepath.Abs(u.HomeDir + path[1:]); err != nil {
			return
		}
	} else if settings.path, err = filepath.Abs(path); err != nil {
		return
	}

	switch b, err = ioutil.ReadFile(settings.path); err {
	case nil:
		if err = json.Unmarshal(b, &settings.cache); err != nil {
			settings.cache = make(map[string]SettingsEntry)
		}
		settings.writable = err == nil
	default:
		settings.cache = make(map[string]SettingsEntry)
		settings.writable = os.IsNotExist(err)
	}
	return
}

// Write method on SettingsCache writes (potentially modified) cached file-type settings back to
// the JSON settings file
func (settings *settingsCache) Write() error {
	settings.mutex.Lock()
	defer settings.mutex.Unlock()

	if !settings.writable {
		return fmt.Errorf("can't write to %q", settings.path)
	}
	b, err := json.MarshalIndent(settings.cache, "", "    ")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(settings.path, b, 0644)
}

// Find method on SettingsCache returns true if file-type signature exists in the settings cache
func (settings *settingsCache) Find(sig string) bool {
	settings.mutex.Lock()
	defer settings.mutex.Unlock()
	_, found := settings.cache[sig]
	return found
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

	if sig != "" {
		settings.cache[sig] = entry
	}
	return entry
}

// Peek returns a digest to identify the CSV (or fixed-field TXT file) at "path". This digest
// consists of a preview slice of raw data rows (without blank or comment lines), a total file row
// estimate, a hint whether the first row is a heading, the comment prefix used (if any), and if a
// CSV, the field separator with trimmed fields of the preview data rows split by it. A file-type
// signature (specifier or heading MD5 hash as available) with any settings info mapped to it
// (like file application type, version and default column selector-map) are also provided.
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

	bf, row, fix, tlen, max, hash := bufio.NewScanner(file), -1, 0, 0, 1, ""
nextLine:
	for row < previewRows && bf.Scan() {
		switch ln := bf.Text(); {
		case len(strings.TrimSpace(ln)) == 0:
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
			switch row++; {
			case row == 2:
				fix = len(ln)
			case len(ln) != fix:
				fix = 0
			}
			tlen += len(ln)
			dig.Preview = append(dig.Preview, ln)
		}
	}
	switch e := bf.Err(); {
	case e != nil:
		panic(fmt.Errorf("problem reading %q (%v)", path, e))
	case row < 3:
		panic(fmt.Errorf("%q needs at least 3 data rows to characterize file", path))
	case row < previewRows:
		dig.Rows = row
	default:
		dig.Rows = int(float64(info.Size())/float64(tlen-len(dig.Preview[0])+row-1)*0.995+0.5) * (row - 1)
	}

nextSep:
	for _, r := range sepSet {
		c, sl, sh := 0, []string{}, []string{}
		for i, ln := range dig.Preview {
			if sl = io.SplitCSV(ln, r); len(sl) <= max || c > 0 && len(sl) != c {
				continue nextSep
			}
			for _, f := range sl {
				if len(f) > maxFieldLen {
					continue nextSep
				}
			}
			if i == 0 {
				sh, c = sl, len(sl)
			}
		}
		if dig.Sep, max, hash = r, c, fmt.Sprintf("%x", md5.Sum([]byte(strings.Join(sh, string(r))))); Settings.Find(hash) {
			break
		}
		qh := make(map[string]int, c)
		for _, h := range sh {
			h = strings.TrimSpace(h)
			if _, e := strconv.ParseFloat(h, 64); e != nil && len(h) > 0 {
				qh[h]++
			}
		}
		if len(qh) != c {
			hash = ""
		}
	}

	switch {
	case dig.Sep == '\x00' && fix == 0:
		panic(fmt.Errorf("cannot determine %q file format", path))
	case hash == "" && (fix > 132 && max < 4 || fix/max > bigFieldLen):
		dig.Sep = '\x00' // ambiguous formats with suspiciously low-density columns
		fallthrough
	case dig.Sep == '\x00':
		dig.Heading = len(dig.Preview[0]) != fix
		dig.Sig, dig.Heading = dig.getFSpec()
	default:
		for _, r := range dig.Preview {
			dig.Split = append(dig.Split, io.SplitCSV(r, dig.Sep))
		}
		if dig.Sig, dig.Heading = hash, hash != ""; !Settings.Find(dig.Sig) {
			if spec := dig.getSpec(); spec != "" {
				dig.Sig, dig.Heading = spec, spec[2] == '{'
			}
		}
	}
	dig.Settings = Settings.Get(dig.Sig)
	return
}

// ReadFixed returns a channel into which a goroutine writes field-maps of fixed-field TXT rows
// from file at "path" keyed by "fcols" (channels also provided for errors and for the consumer to
// signal a halt).  Fields selected by column ranges in "fcols" map are trimmed of blanks; empty
// fields are suppressed; "head" lines, blank lines and those prefixed by "comment" are skipped.
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
		in, ierr, isig := io.ReadLn(path)
		defer close(isig)
		io.HandleSig(sig, &sigv)

		cols, sel, wid, line, algn := map[string]cmapEntry{}, 0, 0, 0, 0
		for ln := range in {
			for line++; ; {
				switch {
				case len(strings.TrimLeft(ln, " ")) == 0:
				case comment != "" && strings.HasPrefix(ln, comment):
				case head:
					head = false
				case wid == 0:
					wid = len(ln)
					if cols, sel = parseCMap(fcols, true, wid); sel == 0 {
						panic(fmt.Errorf("no columns selected by map provided for fixed-field file %q", path))
					}
					continue

				case len(ln) != wid:
					if algn++; line > 200 && float64(algn)/float64(line) > 0.02 {
						panic(fmt.Errorf("excessive column misalignment in fixed-field file %q (>%d rows)", path, algn))
					}
				default:
					m, skip := make(map[string]string, len(cols)), false
					for h, c := range cols {
						f := strings.TrimSpace(ln[c.begin-1 : c.col])
						if len(c.prefix) > 0 {
							for _, p := range c.prefix {
								if skip = strings.HasPrefix(f, p) == !c.inclusive; skip == !c.inclusive {
									break
								}
							}
							if skip {
								break
							} else if c.skip {
								continue
							}
						}
						if len(f) > 0 {
							m[h] = f
						}
					}
					if !skip && len(m) > 0 {
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

// Read returns a channel into which a goroutine writes field-maps of CSV rows from file at "path"
// keyed by column selector-map "cols", or if "" and "head" present, by the heading in the first
// data row (channels also provided for errors and for the consumer to signal a halt). CSV
// separator is "sep", or if \x00, will be inferred. Fields are trimmed of blanks and double-
// quotes (which may enclose separators); empty fields are suppressed; blank lines and those
// prefixed by "comment" are skipped.
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
		in, ierr, isig := io.ReadLn(path)
		defer close(isig)
		io.HandleSig(sig, &sigv)

		vcols, wid, line, algn, skip := make(map[string]cmapEntry, 32), 0, 0, 0, false
		for ln := range in {
			for line++; ; {
				switch {
				case len(strings.TrimSpace(ln)) == 0:
				case comment != "" && strings.HasPrefix(ln, comment):
				case sep == '\x00':
					for _, r := range sepSet {
						if c := len(io.SplitCSV(ln, r)); c > wid {
							wid, sep = c, r
						}
					}
					continue
				case len(vcols) == 0:
					sl, uc, vs := io.SplitCSV(ln, sep), make(map[int]int), 0
					wid = len(sl)
					pc, ps := parseCMap(cols, false, wid)
					for _, c := range pc {
						uc[c.col]++
					}
					for i, h := range sl {
						if h != "" && (ps == 0 || pc[h].col > 0) {
							c := pc[h]
							c.col = i + 1
							vcols[h] = c
						}
					}
					for _, c := range vcols {
						if !c.skip {
							vs++
						}
					}
					switch {
					case ps == 0 && (!head || len(vcols) != wid):
						panic(fmt.Errorf("can't read CSV file %q without column heads in file or map", path))
					case ps == 0 && vs == 0:
						panic(fmt.Errorf("column map skips all columns in CSV file %q", path))
					case ps == 0:
					case vs == ps:
					case vs > 0:
						panic(fmt.Errorf("missing %d column(s) in CSV file %q", ps-vs, path))
					case head:
						panic(fmt.Errorf("column map incompatible with CSV file %q", path))
					case len(uc) < len(pc):
						panic(fmt.Errorf("%d conflicting column(s) in map provided for CSV file %q", len(pc)-len(uc), path))
					default:
						vcols = pc
						continue
					}

				default:
					if b, sl := io.SliceCSV(ln, sep); len(sl)-1 == wid {
						m := make(map[string]string, len(vcols))
						skip, head = false, true
						for h, c := range vcols {
							fs := b[sl[c.col-1]:sl[c.col]]
							f := *(*string)(unsafe.Pointer(&fs)) // avoid new string for ~8% perf gain
							if len(c.prefix) > 0 {
								for _, p := range c.prefix {
									if skip = strings.HasPrefix(f, p) == !c.inclusive; skip == !c.inclusive {
										break
									}
								}
								if skip {
									break
								} else if c.skip {
									continue
								}
							}
							if len(f) > 0 {
								m[h], head = f, head && f == h
							} else {
								head = false
							}
						}
						if !skip && !head && len(m) > 0 {
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