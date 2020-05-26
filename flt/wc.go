package flt

import (
	"strings"

	_ "github.com/sententico/cost/internal/pfax"
)

// WC concurrently filters CSV/fixed-field input for aggregation
func WC(fin chan<- interface{}, in <-chan map[string]string) {
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
			} else if len(ke) < 100 {
				ke[v] = 1
			}
		}
	}
	fin <- km
	return
}
