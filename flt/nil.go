package flt

import (
	"github.com/sententico/cost/csv"
	_ "github.com/sententico/cost/internal/pfax" // stub reference
)

// Nil concurrently pass-through filters CSV/fixed-field inputs for aggregation
func Nil(fin chan<- interface{}, in <-chan map[string]string, dig csv.Digest) {
	for row := range in {
		fin <- row
	}
	return
}
