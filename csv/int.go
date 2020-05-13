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
				if _, ok := head[strings.Trim(s, " ")]; !ok {
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
//		"=h80,h1:HEAD01,f132,f52:20,f126:S :M :L :XL" (heading & field row specs)
//		"=f72,f72:T:F,f20:SKU" (field row specs only)
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

// parseCMap parses a column-map string, returning the resulting map
//   CSV file type column-map syntax:
//		"<head>[:<col>][,<head>[:<col>]]..."
//   examples (in shell use, enclose in single-quotes):
//		"name,age,account number" (columns implicitly identified through file header)
//		"name:1,age:4,account number:13" (explicit column mappings for files with no header)
func parseCMap(cmap string) (m map[string]int) {
	if cmap != "" {
		m = make(map[string]int, 32)
		p, c, h := 0, 0, ""
		for _, t := range strings.Split(cmap, ",") {
			v := strings.Split(t, ":")
			switch c = atoi(v[len(v)-1], -1); {
			case c == -1:
				h, c = strings.Trim(t, " "), p+1
			case c <= 0:
				h, c = strings.Trim(strings.Join(v[:len(v)-1], ":"), " "), p+1
			default:
				h = strings.Trim(strings.Join(v[:len(v)-1], ":"), " ")
			}
			if h != "" {
				m[h], p = c, c
			}
		}
	}
	return
}

// parseFCMap parses a fixed-column-map string, returning the resulting map
//   fixed-field TXT file type column-map syntax:
//		"(<head>|~):<ecol> | <head>:<bcol>:<ecol>[,(<head>|~):<ecol>|<head>:<bcol>:<ecol>]...[,<head>]"
//   examples (in shell use, enclose in single-quotes):
//		"name:20,~:62,age:65,~:122,account number" (column-end reference style with "~" skips)
//		"name:1:20,age:63:65,account number:123:132" (full begin:end column references)
func parseFCMap(fcmap string, wid int) (m map[string][2]int) {
	if fcmap == "" {
		return map[string][2]int{"~raw": {1, wid}}
	}
	m = make(map[string][2]int, 32)
	p, b, e, h := 0, 0, 0, ""
	for _, t := range strings.Split(fcmap, ",") {
		switch v := strings.Split(t, ":"); len(v) {
		case 1:
			if h, e = strings.Trim(v[0], " "), wid; h != "" && h != "~" && p < wid {
				m[h] = [2]int{p + 1, wid}
			}
		case 2:
			if h, e = strings.Trim(v[0], " "), atoi(v[1], -1); h != "" && h != "~" &&
				e > p && e <= wid {
				m[h] = [2]int{p + 1, e}
			}
		default:
			if h, b, e = strings.Trim(v[0], " "), atoi(v[1], -1), atoi(v[2], -1); h != "" && h != "~" &&
				b > 0 && e >= b && e <= wid {
				m[h] = [2]int{b, e}
			}
		}
		p = e
	}
	return
}
