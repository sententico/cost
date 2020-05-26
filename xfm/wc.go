package xfm

import (
	"fmt"
	"sort"
	"strings"

	_ "github.com/sententico/cost/internal/pfax" // stub reference
)

// WC transforms filtered aggregate...
func WC(agg interface{}) {
	// type switch assertions extract concrete types from interface for transformation
	wc := agg.(map[string]map[string]int)
	head := make([]string, 0, len(wc))
	for k := range wc {
		if len(wc[k]) < 1000 {
			head = append(head, k)
		}
	}
	sort.Slice(head, func(i, j int) bool {
		return head[i] < head[j]
	})
	cols := make(map[int][]string, len(head))
	for c, h := range head {
		col := make([]string, 0, len(wc[h]))
		for k := range wc[h] {
			col = append(col, k)
		}
		sort.Slice(col, func(i, j int) bool {
			return wc[h][col[i]] > wc[h][col[j]]
		})
		cols[c] = col
	}

	switch len(head) {
	case 0:
		fmt.Printf("no wc output")
	default:
		fmt.Printf("%v,\n", strings.Join(head, ",,"))
	}
	for r, output := 0, true; output; r++ {
		output = false
		for c, h := range head {
			switch {
			case r >= len(cols[c]) && c == 0:
				fmt.Printf(",")
			case r >= len(cols[c]):
				fmt.Printf(",,")
			case c == 0:
				fmt.Printf("%v,%v", cols[c][r], wc[h][cols[c][r]])
				output = true
			default:
				fmt.Printf(",%v,%v", cols[c][r], wc[h][cols[c][r]])
				output = true
			}
		}
		fmt.Printf("\n")
	}
	return
}
