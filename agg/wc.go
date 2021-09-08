package agg

import (
	_ "github.com/sententico/cost/internal/pfax" // stub reference
)

// WC aggregator; aggregates WC filtered input for transformation
func WC(fin <-chan interface{}) interface{} {
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
