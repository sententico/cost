package csv

import (
	"strconv"
	"strings"
)

const (
	previewRows = 16       // number of preview rows returned by Peek (must be >2)
	sepSet      = ",\t|;:" // priority of separator runes automatically checked if none specified
	maxFieldLen = 256      // maximum field size allowed for Peek to qualify a separator
	bigFieldLen = 36       // mean field length above which CSV column-density is suspiciously low
)

type cmapEntry struct {
	col, begin      int      // column reference (paired with begin column for fixed-field files)
	skip, inclusive bool     // skip: filter-only columns; inclusive: prefix op (exclusive default)
	prefix          []string // prefix operands
}

var commentSet = [...]string{"#", "//", "'"}

// atoi is a helper string-to-int function with selectable default value on error
func atoi(s string, d int) int {
	i, e := strconv.Atoi(s)
	if e != nil {
		return d
	}
	return i
}

// getSpec method on Digest scans file-type cache for CSV file specifier signature matching
// digest, returning specifier if found
//   CSV file-type specifier syntax:
// 		"=<sep><cols>[,<col>[$<len>][:<pfx>[:<pfx>]...]]..." (column lengths/prefixes)
//		"=<sep>{<head>[,<head>]...}" (column heads uniquely identifying file-type)
//   examples:
// 		"=|35,7:INTL:DOM,12$13:20,21$3"
//		"=,120,102$16,17$3:Mon:Tue:Wed:Thu:Fri:Sat:Sun,62$5:S :M :L :XL"
//		"=,{name,age,account number}"
func (dig *Digest) getSpec() (spec string) {
	head := make(map[string]int, len(dig.Split[0]))
	for _, c := range dig.Split[0] {
		head[c]++
	}
nextSpec:
	for _, spec = range Settings.GetSpecs() {
		if !strings.HasPrefix(spec, "="+string(dig.Sep)) {
			continue nextSpec
		}
		switch spec[2] {
		case '{':
			for _, s := range strings.Split(strings.Trim(spec[2:], "{}"), ",") {
				if _, ok := head[strings.TrimSpace(s)]; !ok {
					continue nextSpec
				}
			}
			return
		default:
			for _, s := range dig.Split {
			nextTerm:
				for i, t := range strings.Split(spec[2:], ",") {
					v, c1, c2 := strings.Split(t, ":"), -1, -1
					switch cv := strings.Split(v[0], "$"); len(cv) {
					default:
						c2 = atoi(strings.TrimSpace(cv[1]), -1)
						fallthrough
					case 1:
						c1 = atoi(strings.TrimSpace(cv[0]), -1)
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
			return
		}
	}
	return ""
}

// getFSpec method on Digest scans file-type cache for fixed-field file specifier signature
// matching digest, returning specifier and heading indicator if found
//   fixed-field TXT file type specifier syntax (heading/field lengths/prefixes):
// 		"=(f|h)(<cols>|(<col>:<pfx>[:<pfx>]...))[,(f|h)(<cols>|(<col>:<pfx>[:<pfx>]...))]..."
//   examples:
//		"=h80,h1:HEAD01,f132,f52:20,f126:S :M :L :XL" (heading & field row lengths/prefixes)
//		"=f72,f72:T:F,f20:SKU" (field row length/prefixes only)
func (dig *Digest) getFSpec() (spec string, head bool) {
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
		return spec, dig.Heading || strings.HasPrefix(spec, "=h") || strings.Contains(spec, ",h")
	}
	return "", dig.Heading
}

// parseCMap parses a column-map string for CSV or fixed-field file types of specified width,
// returning map with selected column count
//   column-map syntax for CSV files:
//   	"[!]<head>[:(=|!){<pfx>[:<pfx>]...}][:<col>]
//       [,[!]<head>[:(=|!){<pfx>[:<pfx>]...}][:<col>]]..."
//   column-map syntax for fixed-field TXT files:
//		"[!]<head>[:(=|!){<pfx>[:<pfx>]...}][:<bcol>]:<ecol>
//       [,[!]<head>[:(=|!){<pfx>[:<pfx>]...}][:<bcol>]:<ecol>]..."
//   examples (in shell use, enclose in single-quotes):
//      "name,,,age,,acct num" (implicit columns, with skips)
//		"name:1,age:4,acct num:6" (same, with explicit columns)
//		"name:={James:Mary},,,age,,acct num:!{N/A:00000}" (same with include/exclude filters)
//		"name:20,:62,age:65,:122,acct num:127" (now in a fixed file with implicit begin columns)
//		"name:1:20,age:63:65,!acct num:![N/A:00000:]:123:127" (same but explicit with skip/filter)
func parseCMap(cmap string, fixed bool, wid int) (m map[string]cmapEntry, selected int) {
	switch {
	case cmap == "" && fixed:
		return map[string]cmapEntry{"~raw": {begin: 1, col: wid}}, 1
	case cmap == "":
		return
	}
	m = make(map[string]cmapEntry, 32)
	cursor := 0

	for _, t := range strings.Split(cmap, ",") {
		v, me, a, b, h := strings.Split(t, ":"), cmapEntry{}, 0, 0, ""

		if len(v) > 2 && fixed {
			if me.col, b = atoi(v[len(v)-1], 0), len(v)-1; me.col > 0 {
				if me.begin, b = atoi(v[len(v)-2], 0), len(v)-3; me.begin == 0 || me.begin > me.col {
					me.begin, b = 0, len(v)-2
				}
			}
		} else if len(v) > 1 {
			if me.col, b = atoi(v[len(v)-1], 0), len(v)-2; me.col == 0 {
				b = len(v) - 1
			}
		}
		for a = 1; a < len(v) && !strings.HasPrefix(v[a], "=[") && !strings.HasPrefix(v[a], "!["); a++ {
		}
		if a <= b && strings.HasSuffix(v[b], "]") {
			me.skip, me.inclusive = strings.HasPrefix(v[0], "!"), v[a][0] == '='
			v[a] = v[a][2:]
			v[b] = v[b][:len(v[b])-1]
			me.prefix = v[a : b+1]
		} else if a < len(v) {
			continue
		}
		switch {
		case a <= b:
			h = strings.Join(v[:a], ":")
		case me.begin > 0:
			h = strings.Join(v[:len(v)-2], ":")
		case me.col > 0:
			h = strings.Join(v[:len(v)-1], ":")
		default:
			h = t
		}
		switch {
		case fixed && me.col > cursor && me.begin == 0:
			me.begin = cursor + 1
		case !fixed && me.col == 0:
			me.col = cursor + 1
		}

		switch th := strings.TrimLeft(h, "!"); {
		case me.col == 0 || me.col > wid || me.begin == 0 && fixed:
			continue
		case th != "" && (h[0] != '!' || me.skip):
			m[th] = me
			fallthrough
		default:
			cursor = me.col
		}
	}

	for _, me := range m {
		if !me.skip {
			selected++
		}
	}
	return
}
