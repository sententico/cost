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

// parseCMap parses a CSV column-map string, returning map and head count
//   CSV file type column-map syntax:
//		"(<head>|[(~|=)<pfx>])[:<col>][,(<head>|[(~|=)<pfx>])[:<col>]]..."
//   examples (in shell use, enclose in single-quotes):
//		"name,,,age,,acct num" (columns, with skips, implicitly identified via file header)
//		"name:1,age:4,acct num:6" (explicit column mappings for files with no header)
//		"~N/A:6,name,,,age,,acct num:6" (same with field-prefix row skip)
func parseCMap(cmap string) (m map[string]int, heads int) {
	if cmap == "" {
		return
	}
	m = make(map[string]int, 32)
	h, p, c := "", 0, -1

	for _, t := range strings.Split(cmap, ",") {
		if v := strings.Split(t, ":"); len(v) == 1 {
			h, c = strings.TrimSpace(t), p+1
		} else if c = atoi(v[len(v)-1], -1); c > 0 {
			h = strings.TrimSpace(strings.Join(v[:len(v)-1], ":"))
		} else {
			h, c = strings.TrimSpace(t), p+1
		}
		if h != "" && h != "~" && h != "=" {
			m[h] = c
		}
		p = c
	}

	for h = range m {
		if h[0] != '~' && h[0] != '=' {
			heads++
		}
	}
	return
}

// parseFCMap parses a fixed-field column-map string for a "wid"-column file, returning map and
// head count
//   fixed-field TXT file type column-map syntax:
//		"(<head>|[(~|=)<pfx>])[:<bcol>]:<ecol> [,(<head>|[(~|=)<pfx>])[:<bcol>]:<ecol>]...[,<head>|(~|=)<pfx>]"
//   examples (in shell use, enclose in single-quotes):
//		"name:20,:62,age:65,:122,acct num" (column-end reference style with column skips)
//		"name:1:20,age:63:65,acct num:123:132" (full begin:end column references)
//		"name:1:20,age:63:65,~N/A:123:132,acct num:123:132" (same with field-prefix row skip)
func parseFCMap(fcmap string, wid int) (m map[string][2]int, heads int) {
	if fcmap == "" {
		return map[string][2]int{"raw~": {1, wid}}, 1
	}
	m = make(map[string][2]int, 32)
	h, p, b, e := "", 0, -1, -1

	for _, t := range strings.Split(fcmap, ",") {
		switch v := strings.Split(t, ":"); len(v) {
		case 1:
			h, b, e = strings.TrimSpace(t), -1, -1
		case 2:
			switch b, e = -1, atoi(v[1], -1); {
			case e > 0:
				h = strings.TrimSpace(v[0])
			default:
				h = strings.TrimSpace(t)
			}
		default:
			switch b, e = atoi(v[len(v)-2], -1), atoi(v[len(v)-1], -1); {
			case b > 0 && e > 0:
				h = strings.TrimSpace(strings.Join(v[:len(v)-2], ":"))
			case e > 0:
				h = strings.TrimSpace(strings.Join(v[:len(v)-1], ":"))
			default:
				h, b = strings.TrimSpace(t), -1
			}
		}

		switch {
		case e > wid:
			continue
		case b > 0:
			p = b - 1
		case e > 0:
		case p < wid:
			e = wid
		default:
			continue
		}
		if h != "" && h != "~" && h != "=" && p < e {
			m[h] = [2]int{p + 1, e}
		}
		p = e
	}

	for h = range m {
		if h[0] != '~' && h[0] != '=' {
			heads++
		}
	}
	return
}
