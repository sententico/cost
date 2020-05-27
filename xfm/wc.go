package xfm

import (
	"fmt"
	"sort"
	"strings"

	_ "github.com/sententico/cost/internal/pfax" // stub reference
)

// WC transform; CSV output of WC aggregate
func WC(agg interface{}) {
	wc := agg.(map[string]map[string]int)
	head := make([]string, 0, len(wc))
	for k := range wc {
		// bypass output of wide-ranging values
		if len(wc[k]) < 2000 {
			head = append(head, k)
		}
	}
	if len(head) == 0 {
		fmt.Println("no wc output")
		return
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

	for r, ln := 0, strings.Join(head, ",,")+","; strings.TrimLeft(ln, ",") != ""; r++ {
		fmt.Println(ln)
		for c, h := range head {
			switch {
			case r >= len(cols[c]) && c == 0:
				ln = ","
			case r >= len(cols[c]):
				ln += ",,"
			case c == 0:
				ln = fmt.Sprintf("%q,%v", cols[c][r], wc[h][cols[c][r]])
			default:
				ln += fmt.Sprintf(",%q,%v", cols[c][r], wc[h][cols[c][r]])
			}
		}
	}
}
