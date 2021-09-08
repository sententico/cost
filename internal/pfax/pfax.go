package pfax

import (
	"fmt"

	"github.com/sententico/cost/csv"
)

// Fentry ...
type Fentry struct {
	Flt  func(chan<- interface{}, <-chan map[string]string, csv.Resource)
	Cols string
}

// Fmap ...
type Fmap map[string]Fentry

// Xentry ...
type Xentry struct {
	Descr string
	Xfm   func(interface{})
	Agg   func(<-chan interface{}) interface{}
	Fm    Fmap
}

// Xmap ...
type Xmap map[string]Xentry

// Xname ...
type Xname string

var (
	// Args ...
	Args struct {
		XfmFlag      Xname
		SettingsFlag string
	}
	// Xm ...
	Xm Xmap
)

// String method...
func (x *Xname) String() string {
	return string(*x)
}

// Set method...
func (x *Xname) Set(value string) error {
	if _, ok := Xm[value]; !ok {
		return fmt.Errorf("unknown transform")
	}
	*x = Xname(value)
	return nil
}
