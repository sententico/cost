package csv

import (
	"strconv"
	"strings"
)

const (
	previewRows = 5        // number of preview rows returned by Peek (must be >2)
	sepSet      = ",\t|;:" // priority of separator runes automatically checked if none specified
	maxFieldLen = 256      // maximum field size allowed for Peek to qualify a separator
)

var commentSet = [...]string{"#", "//", "'"}

// atoi is a helper string-to-int function with selectable default value on error
func atoi(s string, d int) int {
	i, e := strconv.Atoi(s)
	if e != nil {
		return d
	}
	return i
}

// getSig scans file type cache for CSV file specifier signature matching "dig", setting Sig in
// same digest; returns true on match
//   CSV file type specifier syntax:
// 		"=<sep><cols>[,<col>[$<len>][:<pfx>[:<pfx>...]]]..."
//   examples:
// 		"=|35,7:INTL:DOM,12$13:20,21$3"
//		"=,120,102$16,17$3:Mon:Tue:Wed:Thu:Fri:Sat:Sun,62$5:S :M :L :XL"
func getSig(dig *Digest) bool {
nextSpec:
	for _, spec := range Settings.GetSpecs() {
		if !strings.HasPrefix(spec, "="+string(dig.Sep)) {
			continue nextSpec
		}
		for _, s := range dig.Split {
		nextTerm:
			for i, t := range strings.Split(spec[2:], ",") {
				v, c1, c2 := strings.Split(t, ":"), -1, -1
				switch cv := strings.Split(v[0], "$"); len(cv) {
				default:
					c2 = atoi(strings.Trim(cv[1], " "), -1)
					fallthrough
				case 1:
					c1 = atoi(strings.Trim(cv[0], " "), -1)
				}
				switch {
				case i == 0 && c1 != len(s):
					continue nextSpec
				case i == 0 || c1 <= 0:
					continue nextTerm
				case c1 > len(s) || c2 >= 0 && c2 != len(s[c1-1]):
					continue nextSpec
				case len(v) == 1:
					continue nextTerm
				}
				for _, pfx := range v[1:] {
					if strings.HasPrefix(s[c1-1], strings.TrimLeft(pfx, " ")) {
						continue nextTerm
					}
				}
				continue nextSpec
			}
		}
		dig.Sig = spec
		return true
	}
	return false
}

// getFSig scans file type cache for fixed-field file specifier signature matching "dig", setting
// Sig and Heading in same digest; returns true on match
//   fixed-field TXT file type specifier syntax:
// 		"=({f|h}{<cols> | <col>:<pfx>[:<pfx>]...})..."
//   examples:
//		"=h80,h1:HEAD01,f132,f52:20,f126:S :M :L :XL"
//		"=f72,f72:T:F,f20:SKU"
func getFSig(dig *Digest) bool {
nextSpec:
	for _, spec := range Settings.GetSpecs() {
		if !strings.HasPrefix(spec, "=h") && !strings.HasPrefix(spec, "=f") {
			continue nextSpec
		}
		for i, p := range dig.Preview {
		nextTerm:
			for _, t := range strings.Split(spec[1:], ",") {
				v := strings.Split(strings.TrimLeft(t, " "), ":")
				c := atoi(strings.TrimLeft(strings.TrimRight(v[0], " "), "hf"), -1)
				switch {
				case v[0] == "" || c <= 0 || !strings.ContainsAny(v[0][0:1], "hf"):
					continue nextTerm
				case (v[0][0] == 'h' && i == 0 || v[0][0] == 'f' && i > 0) && len(v) == 1 && c != len(p):
					continue nextSpec
				case len(v) == 1 || v[0][0] == 'h' && i > 0 || v[0][0] == 'f' && i == 0:
					continue nextTerm
				}
				for _, pfx := range v[1:] {
					if strings.HasPrefix(p[c-1:], pfx) {
						continue nextTerm
					}
				}
				continue nextSpec
			}
		}
		dig.Sig, dig.Heading = spec, dig.Heading || strings.HasPrefix(spec, "=h") || strings.Contains(spec, ",h")
		return true
	}
	return false
}

// parseCMap parses a column-map string, returning the resulting map
//   CSV file type column-map syntax:
//		"(<cnam>[:<col>])..."
//   examples:
//		"name,age,income"
//		"name:1,age:4,income:13"
func parseCMap(cmap string) (m map[string]int) {
	if cmap != "" {
		m = make(map[string]int, 32)
		for i, t := range strings.Split(cmap, ",") {
			switch v := strings.Split(t, ":"); len(v) {
			case 1:
				if n := strings.Trim(v[0], " "); n != "" {
					m[n] = i + 1
				}
			default:
				if n, c := strings.Trim(v[0], " "), atoi(v[1], -1); n != "" && c > 0 {
					m[n] = c
				}
			}
		}
	}
	return
}

// parseFCMap parses a fixed-column-map string, returning the resulting map
//   fixed-field TXT file type column-map syntax:
//		"{{<cnam>|~}:<ecol> | <cnam>:<bcol>:<ecol>}...[<cnam>]"
//   examples:
//		"name:20,~:62,age:65,~:122,income"
//		"name:1:20,age:63:65,income:123:132"
func parseFCMap(fcmap string, wid int) (m map[string][2]int) {
	if fcmap == "" {
		return map[string][2]int{"~raw": {1, wid}}
	}
	m = make(map[string][2]int, 32)
	a, b, p, n := 0, 0, 0, ""
	for _, t := range strings.Split(fcmap, ",") {
		switch v := strings.Split(t, ":"); len(v) {
		case 1:
			if n, b = strings.Trim(v[0], " "), wid; n != "" && n != "~" && p < wid {
				m[n] = [2]int{p + 1, wid}
			}
		case 2:
			if n, b = strings.Trim(v[0], " "), atoi(v[1], -1); n != "" && n != "~" &&
				b > p && b <= wid {
				m[n] = [2]int{p + 1, b}
			}
		default:
			if n, a, b = strings.Trim(v[0], " "), atoi(v[1], -1), atoi(v[2], -1); n != "" && n != "~" &&
				a > 0 && b >= a && b <= wid {
				m[n] = [2]int{a, b}
			}
		}
		p = b
	}
	return
}
