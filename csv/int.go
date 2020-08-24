package csv

import (
	"crypto/md5"
	"fmt"
	"strconv"
	"strings"
	"unsafe"

	"github.com/sententico/cost/internal/io"
)

type (
	// resStat Resource state (see const)
	resStat uint8

	// cmapItem ...
	cmapItem struct {
		col, begin      int      // column reference (paired with begin column for fixed-field files)
		skip, inclusive bool     // skip: filter-only columns; inclusive: prefix op (exclusive default)
		prefix          []string // prefix operands
	}
)

// Resource states
const (
	rsNIL    resStat = iota // initial state
	rsOPEN                  // resource opened, peek-ahead completed
	rsGET                   // channels opened to get resource contents
	rsCLOSED                // resource closed (cannot be re-opened)
)

// general package constants
const (
	previewLines = 24       // maximum preview lines returned in Resource on Open (must be >2)
	sepSet       = ",\t|;:" // priority of separator runes automatically checked if none specified
	maxFieldLen  = 2048     // maximum field size allowed for Peek to qualify a separator
	bigFieldLen  = 36       // mean field length above which CSV column-density is suspiciously low
)

var (
	commentSet = [...]string{"#", "//", "'"}
)

// peekAhead reads initial lines of an opened resource, populating its identification fields. These
// include a preview slice of raw data rows (excluding blank and comment lines), a total resource
// row estimate, an indicator whether the first row is a heading, the comment prefix used (if any),
// and if a CSV type, the field separator with trimmed fields of the preview data rows split by it.
// A format signature (specifier or heading MD5 hash, as available) with any settings info mapped
// to it (like file application type, version and default column selector-map) are also provided.
func (res *Resource) peekAhead() {
	// TODO: improve override handling (Typ, Sep, Cols, Comment, Shebang)
	// comment/shebang may not have (") prefix
	row, fix, tlen, sep, max, hash := -1, 0, 0, '\x00', 1, ""
nextLine:
	for ln := range res.peek {
		switch {
		case len(strings.TrimSpace(ln)) == 0:
		case res.Shebang != "" && strings.HasPrefix(ln, res.Shebang) || res.Comment != "" && strings.HasPrefix(ln, res.Comment):
			if row < 0 {
				row = 0
			}
		case row < 0:
			row = 0
			for _, p := range commentSet {
				if strings.HasPrefix(ln, p) {
					res.Comment, res.Shebang = p, p+"!"
					continue nextLine
				}
			}
			fallthrough
		default:
			switch row++; {
			case row == 2:
				fix = len(ln)
			case len(ln) != fix:
				fix = 0
			}
			tlen += len(ln)
			res.Preview = append(res.Preview, ln)
		}
	}
	switch {
	case len(res.ierr) > 0:
		panic(fmt.Errorf("problem peeking ahead on resource (%v)", <-res.ierr))
	case row < 1:
		res.Typ, res.Rows = RTempty, 0
		return
	case res.finfo != nil:
		res.Rows = int(float64(res.finfo.Size())/float64(tlen-len(res.Preview[0])+row-1)*0.995+0.5) * (row - 1)
	default:
		res.Rows = -1
	}

nextSep:
	for _, r := range string(res.Sep) + sepSet {
		c, sl, sh := 0, []string{}, []string{}
		for i, ln := range res.Preview {
			if sl = io.SplitCSV(ln, r); len(sl) <= max || c > 0 && len(sl) != c {
				continue nextSep
			}
			for _, f := range sl {
				if len(f) > maxFieldLen { // TODO: remove or modify this check?
					continue nextSep
				}
			}
			if i == 0 {
				sh, c = sl, len(sl)
			}
		}
		if sep, max, hash = r, c, fmt.Sprintf("%x", md5.Sum([]byte(strings.Join(sh, string(r))))); res.SettingsCache.Find(hash) {
			break
		}
		qh := make(map[string]int, c)
		for _, h := range sh {
			h = strings.TrimSpace(h)
			if _, e := strconv.ParseFloat(h, 64); e != nil && len(h) > 0 {
				qh[h]++
			}
		}
		if len(qh) != c {
			hash = ""
		}
		if sep == res.Sep {
			break
		}
	}

	switch res.Sep = sep; {
	case res.Sep == '\x00' && fix == 0:
		// unknown resource type
		res.Typ = RTunk
	case res.Sep != '\x00' && fix != 0 && (res.Typ == RTfixed || res.Typ == RTunk && hash == "" && fix/max > bigFieldLen):
		// ambigious resource type, but evidence for CSV is weak
		fallthrough
	case res.Sep == '\x00':
		// fixed-field resource type
		res.Typ, res.Heading = RTfixed, len(res.Preview[0]) != fix
		res.Sig, res.Heading = res.findFSpec()
	default:
		// CSV resource type
		for _, r := range res.Preview {
			res.Split = append(res.Split, io.SplitCSV(r, res.Sep))
		}
		if res.Typ, res.Sig, res.Heading = RTcsv, hash, hash != ""; !res.SettingsCache.Find(res.Sig) {
			if spec := res.findSpec(); spec != "" {
				res.Sig, res.Heading = spec, spec[2] == '{'
			}
		}
	}
	if res.SettingsCache != nil {
		if res.Settings = res.SettingsCache.Get(res.Sig); res.Cols == "" {
			res.Cols = res.Settings.Cols
		}
	}
	res.Heads = res.getHeads()
}

// getCSV method on Resource reads CSV rows, writing them to "out" channel once converted into
// key-value maps as specified in Cols until CSV input is exhausted or "sig" indicates a halt
func (res *Resource) getCSV() {
	vcols, wid, line, algn, skip := make(map[string]cmapItem, 32), 0, 0, 0, false
	head := res.Heading
	for ln := range res.in {
		for line++; ; {
			switch {
			case len(strings.TrimSpace(ln)) == 0:
			case res.Shebang != "" && strings.HasPrefix(ln, res.Shebang):
				select {
				case res.out <- map[string]string{"~meta": ln[len(res.Shebang):], "~line": strconv.Itoa(line)}:
				case <-res.sig:
					return
				}
			case res.Comment != "" && strings.HasPrefix(ln, res.Comment):
			case len(vcols) == 0:
				sl, uc, vs := io.SplitCSV(ln, res.Sep), make(map[int]int), 0
				wid = len(sl)
				pc, ps := parseCMap(res.Cols, false, wid)
				for _, c := range pc {
					uc[c.col]++
				}
				for i, h := range sl {
					if h != "" && (ps == 0 || pc[h].col > 0) {
						c := pc[h]
						c.col = i + 1
						vcols[h] = c
					}
				}
				for _, c := range vcols {
					if !c.skip {
						vs++
					}
				}
				switch {
				case ps == 0 && (!head || len(vcols) != wid):
					panic(fmt.Errorf("can't read CSV resource without internal column heads or map"))
				case ps == 0 && vs == 0:
					panic(fmt.Errorf("column map skips all columns in CSV resource"))
				case ps == 0:
				case vs == ps:
				case vs > 0:
					panic(fmt.Errorf("missing %d column(s) in CSV resource", ps-vs))
				case head:
					panic(fmt.Errorf("column map incompatible with CSV resource"))
				case len(uc) < len(pc):
					panic(fmt.Errorf("%d conflicting column(s) in map provided for CSV resource", len(pc)-len(uc)))
				default:
					vcols = pc
					continue
				}

			default:
				if b, sl := io.SliceCSV(ln, res.Sep, wid); len(sl)-1 == wid {
					m := make(map[string]string, len(vcols))
					skip, head = false, true
					for h, c := range vcols {
						fs := b[sl[c.col-1]:sl[c.col]]
						f := *(*string)(unsafe.Pointer(&fs)) // avoid new string for ~8% perf gain
						if len(c.prefix) > 0 {
							for _, p := range c.prefix {
								if skip = strings.HasPrefix(f, p) == !c.inclusive; skip == !c.inclusive {
									break
								}
							}
							if skip {
								break
							} else if c.skip {
								continue
							}
						}
						if len(f) > 0 {
							m[h], head = f, head && f == h
						} else {
							head = false
						}
					}
					if !skip && !head && len(m) > 0 {
						m["~line"] = strconv.Itoa(line)
						select {
						case res.out <- m:
						case <-res.sig:
							return
						}
					}
				} else if algn++; line > 200 && float64(algn)/float64(line) > 0.02 {
					panic(fmt.Errorf("excessive column misalignment in CSV resource (>%d rows)", algn))
				}
			}
			break
		}
	}
}

// getFixed method on Resource reads fixed-field rows, writing them to "out" channel once converted
// into key-value maps as specified in Cols until fixed-field input is exhausted or "sig" indicates
// a halt
func (res *Resource) getFixed() {
	head, cols, sel, wid, line, algn := res.Heading, map[string]cmapItem{}, 0, 0, 0, 0
	for ln := range res.in {
		for line++; ; {
			switch {
			case len(strings.TrimLeft(ln, " ")) == 0:
			case res.Shebang != "" && strings.HasPrefix(ln, res.Shebang):
				select {
				case res.out <- map[string]string{"~meta": ln[len(res.Shebang):], "~line": strconv.Itoa(line)}:
				case <-res.sig:
					return
				}
			case res.Comment != "" && strings.HasPrefix(ln, res.Comment):
			case head:
				head = false
			case wid == 0:
				wid = len(ln)
				if cols, sel = parseCMap(res.Cols, true, wid); sel == 0 {
					panic(fmt.Errorf("no columns selected by map provided for fixed-field resource"))
				}
				continue

			case len(ln) != wid:
				if algn++; line > 200 && float64(algn)/float64(line) > 0.02 {
					panic(fmt.Errorf("excessive column misalignment in fixed-field resource (>%d rows)", algn))
				}
			default:
				m, skip := make(map[string]string, len(cols)), false
				for h, c := range cols {
					f := strings.TrimSpace(ln[c.begin-1 : c.col])
					if len(c.prefix) > 0 {
						for _, p := range c.prefix {
							if skip = strings.HasPrefix(f, p) == !c.inclusive; skip == !c.inclusive {
								break
							}
						}
						if skip {
							break
						} else if c.skip {
							continue
						}
					}
					if len(f) > 0 {
						m[h] = f
					}
				}
				if !skip && len(m) > 0 {
					m["~line"] = strconv.Itoa(line)
					select {
					case res.out <- m:
					case <-res.sig:
						return
					}
				}
			}
			break
		}
	}
}

// findSpec method on Resource scans settings cache for CSV resource specifier signature matching
// resource, returning specifier if found
//   CSV resource type format specifier syntax:
// 		"=<sep><cols>[,<col>[$<len>][:<pfx>[:<pfx>]...]]..." (column lengths/prefixes)
//		"=<sep>{<head>[,<head>]...}" (column heads uniquely identifying format)
//   examples:
// 		"=|35,7:INTL:DOM,12$13:20,21$3"
//		"=,120,102$16,17$3:Mon:Tue:Wed:Thu:Fri:Sat:Sun,62$5:S :M :L :XL"
//		"=,{name,age,account number}"
func (res *Resource) findSpec() (spec string) {
	head := make(map[string]int, len(res.Split[0]))
	for _, c := range res.Split[0] {
		head[c]++
	}
nextSpec:
	for _, spec = range res.SettingsCache.GetSpecs() {
		if !strings.HasPrefix(spec, "="+string(res.Sep)) {
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
			for _, s := range res.Split {
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

// findFSpec method on Resource scans format cache for fixed-field resource specifier signature
// matching resource, returning specifier and heading indicator if found
//   fixed-field TXT resource type specifier syntax (heading/field lengths/prefixes):
// 		"=(f|h)(<cols>|(<col>:<pfx>[:<pfx>]...))[,(f|h)(<cols>|(<col>:<pfx>[:<pfx>]...))]..."
//   examples:
//		"=h80,h1:HEAD01,f132,f52:20,f126:S :M :L :XL" (heading & field row lengths/prefixes)
//		"=f72,f72:T:F,f20:SKU" (field row length/prefixes only)
func (res *Resource) findFSpec() (spec string, head bool) {
nextSpec:
	for _, spec := range res.SettingsCache.GetSpecs() {
		if !strings.HasPrefix(spec, "=h") && !strings.HasPrefix(spec, "=f") {
			continue nextSpec
		}
		for i, p := range res.Preview {
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
		return spec, res.Heading || strings.HasPrefix(spec, "=h") || strings.Contains(spec, ",h")
	}
	return "", res.Heading
}

// getHeads method on Resource returns a column heads slice in lexical order from column map Col
// or from the resource itself if a CSV type
func (res *Resource) getHeads() (heads []string) {
	var th []string
	if res.Cols == "" && res.Typ == RTcsv && res.Heading {
		th = res.Split[0]
	} else if res.Cols == "" {
		return
	} else {
		for _, t := range strings.Split(res.Cols, ",") {
			if h := strings.Split(t, ":")[0]; h != "" && !strings.HasPrefix(h, "!") {
				th = append(th, h)
			}
		}
	}
	m := make(map[string]bool)
	for _, h := range th {
		if !m[h] {
			heads = append(heads, h)
			m[h] = true
		}
	}
	return
}

// parseCMap parses a column-map string for CSV or fixed-field resource types of specified width,
// returning map with selected column count
//   column-map syntax for CSV resource types:
//		"[!]<head>[:(=|!){<pfx>[:<pfx>]...}][:<col>]
//		 [,[!]<head>[:(=|!){<pfx>[:<pfx>]...}][:<col>]]..."
//   column-map syntax for fixed-field TXT resource types:
//		"[!]<head>[:(=|!){<pfx>[:<pfx>]...}][:<bcol>]:<ecol>
//		 [,[!]<head>[:(=|!){<pfx>[:<pfx>]...}][:<bcol>]:<ecol>]..."
//   examples (in shell use, enclose in single-quotes):
//		"name,,,age,,acct num" (implicit columns, with skips)
//		"name:1,age:4,acct num:6" (same, with explicit columns)
//		"name:={James:Mary},,,age,,acct num:!{N/A:00000}" (same with inclusive/exclusive filters)
//		"name:20,:62,age:65,:122,acct num:127" (now in a fixed file with implicit begin columns)
//		"name:1:20,age:63:65,!acct num:![N/A:00000:]:123:127" (same but explicit with skip/filter)
func parseCMap(cmap string, fixed bool, wid int) (m map[string]cmapItem, selected int) {
	switch {
	case cmap == "" && fixed:
		return map[string]cmapItem{"~raw": {begin: 1, col: wid}}, 1
	case cmap == "":
		return
	}
	m = make(map[string]cmapItem, 32)
	cursor := 0

	for _, t := range strings.Split(cmap, ",") {
		v, mi, a, b, h := strings.Split(t, ":"), cmapItem{}, 0, 0, ""

		if len(v) > 2 && fixed {
			if mi.col, b = atoi(v[len(v)-1], 0), len(v)-1; mi.col > 0 {
				if mi.begin, b = atoi(v[len(v)-2], 0), len(v)-3; mi.begin == 0 || mi.begin > mi.col {
					mi.begin, b = 0, len(v)-2
				}
			}
		} else if len(v) > 1 {
			if mi.col, b = atoi(v[len(v)-1], 0), len(v)-2; mi.col == 0 {
				b = len(v) - 1
			}
		}
		for a = 1; a < len(v) && !strings.HasPrefix(v[a], "={") && !strings.HasPrefix(v[a], "!{"); a++ {
		}
		if a <= b && strings.HasSuffix(v[b], "}") {
			mi.skip, mi.inclusive = strings.HasPrefix(v[0], "!"), v[a][0] == '='
			v[a] = v[a][2:]
			v[b] = v[b][:len(v[b])-1]
			mi.prefix = v[a : b+1]
		} else if a < len(v) {
			continue
		}
		switch {
		case a <= b:
			h = strings.Join(v[:a], ":")
		case mi.begin > 0:
			h = strings.Join(v[:len(v)-2], ":")
		case mi.col > 0:
			h = strings.Join(v[:len(v)-1], ":")
		default:
			h = t
		}
		switch {
		case fixed && mi.col > cursor && mi.begin == 0:
			mi.begin = cursor + 1
		case !fixed && mi.col == 0:
			mi.col = cursor + 1
		}

		switch th := strings.TrimLeft(h, "!"); {
		case mi.col == 0 || mi.col > wid || mi.begin == 0 && fixed:
			continue
		case th != "" && (h[0] != '!' || mi.skip):
			m[th] = mi
			fallthrough
		default:
			cursor = mi.col
		}
	}

	for _, me := range m {
		if !me.skip {
			selected++
		}
	}
	return
}

// atoi is a helper string-to-int function with selectable default value on error
func atoi(s string, d int) int {
	i, e := strconv.Atoi(s)
	if e != nil {
		return d
	}
	return i
}
