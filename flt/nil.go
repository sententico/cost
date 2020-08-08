package flt

import (
	"github.com/sententico/cost/csv"
	_ "github.com/sententico/cost/internal/pfax" // stub reference
)

// Nil concurrent filter; CSV/fixed-field input pass-through for aggregation
func Nil(fin chan<- interface{}, in <-chan map[string]string, res csv.Resource) {
	for row := range in {
		fin <- row
	}
}
