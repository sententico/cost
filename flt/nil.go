package flt

import (
	_ "github.com/sententico/cost/internal/pfax"
)

// Nil concurrently pass-through filters CSV/fixed-field inputs for aggregation
func Nil(fin chan<- interface{}, in <-chan map[string]string) {
	for row := range in {
		fin <- row
	}
	return
}
