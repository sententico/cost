package csv

import (
	"fmt"
	"io"
	"os"
	"time"

	iio "github.com/sententico/cost/internal/io"
)

type (
	// ResTyp ...
	ResTyp uint8

	// Resource ...
	Resource struct {
		Name     string        // resource name (pathname, ...)
		Typ      ResTyp        // resource type
		Cols     string        // resource column map
		Preview  []string      // preview rows (excluding blank & comment lines)
		Rows     int           // estimated total resource rows (-1 if unknown)
		Comment  string        // comment line prefix
		Sep      rune          // field separator rune (for CSV resources)
		Split    [][]string    // trimmed fields of preview rows split by "Sep" (if CSV)
		Heading  bool          // first row is a heading
		Sig      string        // format signature (specifier or heading MD5 hash, if determined)
		Settings SettingsEntry // format settings located by signature in settings-file (if found)
		stat     resStat
		file     *os.File
		finfo    os.FileInfo
		peek, in <-chan string
		ierr     <-chan error
		isig     chan<- int
		out      chan map[string]string
		err      chan error
		sig      chan int
	}

	// SettingsEntry contains information for a resource format cached from the settings file under
	// its signature (specifier or heading MD5 hash)
	SettingsEntry struct {
		Cols   string    // column map
		Format string    // format identifier
		Ver    string    // format version
		Date   time.Time // entry update timestamp
		Lock   bool      // entry locked to automatic updates
	}
)

// Resource type constants
const (
	RTunk   ResTyp = iota // unknown/indeterminate
	RTcsv                 // CSV
	RTfixed               // fixed-field
)

// Open method on Resource ...
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
		if res.file, e = os.Open(res.Name); e != nil {
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

// Get method on Resource ...
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

// Close method on Resource ...
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
