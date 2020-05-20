package io

type st4180 uint8

const (
	stSEP st4180 = iota
	stENCL
	stESC
	stPFX
	stIFX
	stSFX
)

// SliceCSV returns buffer with blank-trimmed field slices for "csv" split by "sep", using a safe
// but tolerant implementation of RFC 4180
func SliceCSV(csv string, sep rune) ([]byte, []int) {
	buf, sl, st, slen := make([]byte, 0, len(csv)), make([]int, 1, 4+len(csv)/4), stSEP, 0
	for _, r := range csv {
		if r > '\x7e' || r != '\x09' && r < '\x20' {
			continue
		}
		switch st {
		case stSEP:
			switch r {
			case sep:
				sl = append(sl, len(buf))
			case '"':
				st = stENCL
			case ' ':
				st = stPFX
			default:
				buf, st = append(buf, byte(r)), stIFX
			}
		case stENCL:
			switch r {
			case '"':
				st = stESC
			default:
				buf = append(buf, byte(r))
			}
		case stESC:
			switch r {
			case sep:
				sl, st = append(sl, len(buf)), stSEP
			default:
				buf, st = append(buf, byte(r)), stENCL
			}
		case stPFX:
			switch r {
			case sep:
				sl, st = append(sl, len(buf)), stSEP
			case ' ':
			default:
				buf, st = append(buf, byte(r)), stIFX
			}
		case stIFX:
			switch r {
			case sep:
				sl, st = append(sl, len(buf)), stSEP
			case ' ':
				buf, slen, st = append(buf, byte(r)), len(buf), stSFX
			default:
				buf = append(buf, byte(r))
			}
		case stSFX:
			switch r {
			case sep:
				sl, buf, st = append(sl, slen), buf[:slen], stSEP
			case ' ':
				buf = append(buf, byte(r))
			default:
				buf, st = append(buf, byte(r)), stIFX
			}
		}
	}
	if st == stSFX {
		return buf, append(sl, slen)
	}
	return buf, append(sl, len(buf))
}

// SplitCSV returns blank-trimmed fields in "csv" split by "sep", using a safe but tolerant
// implementation of RFC 4180
func SplitCSV(csv string, sep rune) []string {
	buf, sl := SliceCSV(csv, sep)
	fields := make([]string, 0, len(sl))
	for i := 1; i < len(sl); i++ {
		fields = append(fields, string(buf[sl[i-1]:sl[i]]))
	}
	return fields
}
