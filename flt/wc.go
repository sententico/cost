package flt

import (
	"strings"

	"github.com/sententico/cost/csv"
	_ "github.com/sententico/cost/internal/pfax" // stub reference
)

// WC concurrent filter; counts CSV/fixed-field values per column
func WC(fin chan<- interface{}, in <-chan map[string]string, res csv.Resource) {
	var ke map[string]int
	km, ok := make(map[string]map[string]int), false
	for row := range in {
		for k, v := range row {
			if strings.HasPrefix(k, "~") {
				continue
			}
			if ke, ok = km[k]; !ok {
				ke = make(map[string]int)
				km[k] = ke
			}
			if _, ok = ke[v]; ok {
				ke[v]++
			} else if len(ke) < 2000 {
				ke[v] = 1
			}
		}
	}
	fin <- km
}
