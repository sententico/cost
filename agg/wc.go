package agg

import (
	_ "github.com/sententico/cost/internal/pfax" // stub reference
)

// WC aggregates filtered input for transformation
func WC(fin <-chan interface{}) interface{} {
	// type switch assertions extract concrete types from interface for transformation
	wc := make(map[string]map[string]int)
	for fr := range fin {
		for k, ke := range fr.(map[string]map[string]int) {
			if wce, ok := wc[k]; ok {
				for v, c := range ke {
					wce[v] += c
				}
			} else {
				wc[k] = ke
			}
		}
	}
	return wc
}
