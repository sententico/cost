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
		Location      string    // resource location (pathname, ...)
		Typ           ResTyp    // resource type
		Cols          string    // resource column map
		Comment       string    // comment line prefix
		Shebang       string    // metadata line prefix (Comment + "!" default)
		Sep           rune      // field separator rune (for CSV resources)
		SettingsCache *Settings // format Settings resource cache

		Preview  []string     // preview rows (excluding blank & comment lines)
		Split    [][]string   // trimmed fields of Preview rows split by Sep (if CSV)
		Heads    []string     // column heads from column map
		Heading  bool         // first row is a heading
		Rows     int          // estimated total resource rows (-1 if unknown)
		Sig      string       // format signature (specifier or heading MD5 hash, if determined)
		Settings SettingsItem // format settings matched to Sig in SettingsCache (if found)

		stat     resStat
		reader   io.ReadCloser
		finfo    os.FileInfo
		peek, in <-chan string
		ierr     <-chan error
		isig     chan<- int
		out      chan map[string]string
		err      chan error
		sig      chan int
	}

	// SettingsItem contains Resource format information & settings retrieved by its signature
	// (specifier or heading MD5 hash) from the Settings cache
	SettingsItem struct {
		Cols   string    // column map default
		Format string    // format name
		Ver    string    // format version
		Date   time.Time // entry update timestamp
		Lock   bool      // entry locked to automatic updates
	}

	// Settings cache maps format settings by Resource signature (specifier or heading MD5 hash)
	Settings struct {
		Location string     // settings location (pathname, ...)
		writable bool       // true if cache is writable
		mutex    sync.Mutex // mutex to allow concurrent cache access
		cache    map[string]SettingsItem
	}
)

// Resource type constants
const (
	RTunk   ResTyp = iota // unknown/indeterminate content
	RTempty               // unknown/no content
	RTcsv                 // CSV
	RTfixed               // fixed-field
)

// Open method on Resource populates resource fields for identification and prepares resource
// for Get method extraction. If a SettingsCache is specified, known resource formats can be
// automatically be identified in Settings.
func (res *Resource) Open(r io.ReadCloser) (e error) {
	switch res.stat {
	case rsOPEN, rsGET:
		return fmt.Errorf("resource already open")
	case rsCLOSED:
		return fmt.Errorf("resource must be uninitialized")
	}
	defer func() {
		if i := recover(); i != nil {
			e = i.(error)
			if res.reader != nil {
				res.reader.Close()
				res.reader = nil
			}
			if res.isig != nil {
				close(res.isig)
				res.isig = nil
			}
		}
	}()

	if r == nil {
		if f, e := os.Open(iio.ResolveName(res.Location)); e != nil {
			panic(e)
		} else if res.finfo, e = f.Stat(); e != nil {
			f.Close()
			panic(e)
		} else {
			r = f
		}
	}
	res.reader = r

	res.peek, res.in, res.ierr, res.isig = iio.ReadLn(r, previewLines)
	res.peekAhead()
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

	res.stat, res.Heads = rsGET, res.getHeads()
	res.out, res.err, res.sig = make(chan map[string]string, 64), make(chan error, 1), make(chan int)
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
		case RTempty:
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
	res.reader.Close()
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
		switch file, err := os.Open(iio.ResolveName(set.Location)); err {
		case nil:
			defer file.Close()
			r = file
		default:
			set.cache = make(map[string]SettingsItem)
			set.writable = os.IsNotExist(err)
			return
		}
	}

	switch b, err := io.ReadAll(r); err {
	case nil:
		if err = json.Unmarshal(b, &set.cache); err != nil {
			set.cache = make(map[string]SettingsItem)
		}
		set.writable = err == nil
	default:
		set.cache = make(map[string]SettingsItem)
	}
}

// Sync method on Settings writes (potentially modified) cached format settings back to the JSON
// settings resource; this implementation does not properly sync (file overwrite only).
func (set *Settings) Sync() error {
	if set == nil || set.cache == nil || set.Location == "" {
		return fmt.Errorf("can't write settings cache")
	} else if !set.writable {
		return fmt.Errorf("can't write settings cache to %q", set.Location)
	}
	defer set.mutex.Unlock()
	set.mutex.Lock()

	b, err := json.MarshalIndent(set.cache, "", "\t")
	if err != nil {
		return fmt.Errorf("can't write settings cache to %q (%v)", set.Location, err)
	}
	return ioutil.WriteFile(iio.ResolveName(set.Location), b, 0644)
}

// Find method on Settings returns true if format signature exists in the cache.
func (set *Settings) Find(sig string) (found bool) {
	if set == nil || set.cache == nil || sig == "" {
		return
	}
	defer set.mutex.Unlock()
	set.mutex.Lock()

	_, found = set.cache[sig]
	return
}

// Get method on Settings returns cache item located by format signature.
func (set *Settings) Get(sig string) (i SettingsItem) {
	if set == nil || set.cache == nil || sig == "" {
		return
	}
	defer set.mutex.Unlock()
	set.mutex.Lock()

	i, _ = set.cache[sig]
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

// Set method on Settings returns item set (added or updated) in cache indexed under format
// signature.
func (set *Settings) Set(sig string, item *SettingsItem) *SettingsItem {
	if set == nil || set.cache == nil || sig == "" {
		return nil
	}
	defer set.mutex.Unlock()
	set.mutex.Lock()

	set.cache[sig] = *item
	return item
}
