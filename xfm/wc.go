package xfm

import (
	"fmt"

	_ "github.com/sententico/cost/internal/pfax"
)

// WC transforms filtered aggregate...
func WC(agg interface{}) {
	// type switch assertions extract concrete types from interface for transformation
	fmt.Println("wc transform stub", agg.(map[string]map[string]int))
	return
}
