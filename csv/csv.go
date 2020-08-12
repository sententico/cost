package csv

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"

	iio "github.com/sententico/cost/internal/io"
)

type (
	// ResTyp specifies a Resource type
	ResTyp uint8

	// Resource contains data and metadata for supported package types (CSV, fixed-field, ...)
	Resource struct {
		Name          string        // resource name (pathname, ...)
		Typ           ResTyp        // resource type
		Cols          string        // resource column map
		Preview       []string      // preview rows (excluding blank & comment lines)
		Rows          int           // estimated total resource rows (-1 if unknown)
		Comment       string        // comment line prefix
		Shebang       string        // metadata line prefix (Comment + "!" default)
		Sep           rune          // field separator rune (for CSV resources)
		Split         [][]string    // trimmed fields of Preview rows split by Sep (if CSV)
		Heading       bool          // first row is a heading
		SettingsCache *Settings     // format Settings resource cache
		Sig           string        // format signature (specifier or heading MD5 hash, if determined)
		Settings      SettingsEntry // format settings located by Sig in Settings cache (if found)
		stat          resStat
		file          *os.File
		finfo         os.FileInfo
		peek, in      <-chan string
		ierr          <-chan error
		isig          chan<- int
		out           chan map[string]string
		err           chan error
		sig           chan int
	}

	// SettingsEntry contains Resource format information & settings retrieved by its signature
	// (specifier or heading MD5 hash) from the Settings cache
	SettingsEntry struct {
		Cols   string    // column map default
		Format string    // format name
		Ver    string    // format version
		Date   time.Time // entry update timestamp
		Lock   bool      // entry locked to automatic updates
	}

	// Settings cache maps format settings by Resource signature (specifier or heading MD5 hash)
	Settings struct {
		Name     string     // settings resource name (pathname, ...)
		writable bool       // true if cache is writable
		mutex    sync.Mutex // mutex to allow concurrent cache access
		cache    map[string]SettingsEntry
	}
)

// Resource type constants
const (
	RTunk   ResTyp = iota // unknown/indeterminate
	RTcsv                 // CSV
	RTfixed               // fixed-field
)

// Open method on Resource populates resource fields for identification and prepares resource
// for Get method extraction. If a SettingsCache is specified, known resource formats can be
// automatically be identified in Settings.
func (res *Resource) Open(r io.Reader) (e error) {
	switch res.stat {
	case rsOPEN, rsGET:
		return fmt.Errorf("resource already open")
	case rsCLOSED:
		return fmt.Errorf("resource must be uninitialized")
	}
	defer func() {
		if i := recover(); i != nil {
			e = i.(error)
			res.file.Close()
			if res.isig != nil {
				close(res.isig)
				res.isig = nil
			}
		}
	}()

	if r == nil {
		if res.file, e = os.Open(resolveName(res.Name)); e != nil {
			panic(e)
		}
		if res.finfo, e = res.file.Stat(); e != nil {
			panic(e)
		}
		r = res.file
	}

	res.peek, res.in, res.ierr, res.isig = iio.ReadLn(r, previewLines)
	if res.peekAhead(); len(res.ierr) > 0 {
		panic(fmt.Errorf("peek-ahead error (%v)", <-res.ierr))
	}
	res.stat = rsOPEN
	return nil
}

// Get method on Resource returns a receive channel over which the consumer may iterate and an
// error channel which should be checked once receive channel is closed. Key-value maps are
// returned as specified in Cols.
func (res *Resource) Get() (<-chan map[string]string, <-chan error) {
	switch res.stat {
	case rsNIL, rsCLOSED:
		out, err := make(chan map[string]string, 1), make(chan error, 1)
		err <- fmt.Errorf("resource not open")
		close(err)
		close(out)
		return out, err
	case rsGET:
		return res.out, res.err
	}

	res.stat, res.out, res.err, res.sig = rsGET, make(chan map[string]string, 64), make(chan error, 1), make(chan int)
	go func() {
		defer func() {
			if e := recover(); e != nil {
				res.err <- e.(error)
			}
			close(res.isig)
			close(res.err)
			close(res.out)
		}()

		switch res.Typ {
		case RTcsv:
			res.getCSV()
		case RTfixed:
			res.getFixed()
		default:
			panic(fmt.Errorf("unknown resource type"))
		}
		if e := <-res.ierr; e != nil {
			panic(fmt.Errorf("problem reading resource (%v)", e))
		}
	}()
	return res.out, res.err
}

// Close method on Resource closes resource and signals termination of upstream flow; resource
// may not be re-opened.
func (res *Resource) Close() error {
	switch res.stat {
	case rsNIL, rsCLOSED:
		return fmt.Errorf("resource not open")
	case rsOPEN:
		close(res.isig)
	case rsGET:
		close(res.sig)
	}
	res.file.Close()
	res.stat = rsCLOSED
	return nil
}

// Cache method on Settings reads JSON-encoded format settings resource into cache.
func (set *Settings) Cache(r io.Reader) {
	if set == nil {
		return
	}
	defer set.mutex.Unlock()
	if set.mutex.Lock(); r == nil {
		switch file, e := os.Open(resolveName(set.Name)); e {
		case nil:
			defer file.Close()
			r = file
		default:
			set.cache = make(map[string]SettingsEntry)
			set.writable = os.IsNotExist(e)
			return
		}
	}

	switch b, e := ioutil.ReadAll(r); e {
	case nil:
		if e = json.Unmarshal(b, &set.cache); e != nil {
			set.cache = make(map[string]SettingsEntry)
		}
		set.writable = e == nil
	default:
		set.cache = make(map[string]SettingsEntry)
	}
}

// Sync method on Settings writes (potentially modified) cached format settings back to the JSON
// settings resource; this implementation does not properly sync (file overwrite only).
func (set *Settings) Sync() error {
	if set == nil || set.cache == nil || set.Name == "" {
		return fmt.Errorf("can't write settings cache")
	} else if !set.writable {
		return fmt.Errorf("can't write settings cache to %q", set.Name)
	}
	defer set.mutex.Unlock()
	set.mutex.Lock()

	b, e := json.MarshalIndent(set.cache, "", "    ")
	if e != nil {
		return fmt.Errorf("can't write settings cache to %q (%v)", set.Name, e)
	}
	return ioutil.WriteFile(resolveName(set.Name), b, 0644)
}

// Find method on Settings returns true if format signature exists in the cache.
func (set *Settings) Find(sig string) (found bool) {
	if set == nil || set.cache == nil {
		return
	}
	defer set.mutex.Unlock()
	set.mutex.Lock()

	_, found = set.cache[sig]
	return
}

// Get method on Settings returns cache entry located by format signature.
func (set *Settings) Get(sig string) (e SettingsEntry) {
	if set == nil || set.cache == nil {
		return
	}
	defer set.mutex.Unlock()
	set.mutex.Lock()

	e, _ = set.cache[sig]
	return
}

// GetSpecs method on Settings returns list of format specifications in the cache.
func (set *Settings) GetSpecs() (specs []string) {
	if set == nil || set.cache == nil {
		return
	}
	defer set.mutex.Unlock()
	set.mutex.Lock()

	for sig := range set.cache {
		if strings.HasPrefix(sig, "=") && len(sig) > 2 {
			specs = append(specs, sig)
		}
	}
	return
}

// Set method on Settings returns entry set (added or updated) in cache indexed under format
// signature.
func (set *Settings) Set(sig string, entry *SettingsEntry) *SettingsEntry {
	if set == nil || set.cache == nil || sig == "" {
		return nil
	}
	defer set.mutex.Unlock()
	set.mutex.Lock()

	set.cache[sig] = *entry
	return entry
}
