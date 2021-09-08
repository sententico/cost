package io

type state uint8

const (
	stSEP state = iota
	stENCL
	stESC
	stPFX
	stIFX
	stSFX
)

// SliceCSV returns buffer with blank-trimmed field slices for "csv" split by "sep", using a safe
// but tolerant implementation of RFC 4180
func SliceCSV(csv string, sep rune, expected int) ([]byte, []uint16) {
	if len(csv) > 0xffff {
		csv = csv[:0xffff]
	}
	if expected > len(csv) {
		expected = len(csv) + 1
	}
	var slen uint16
	buf, sl, st := make([]byte, 0, len(csv)-expected+1), make([]uint16, 1, expected+1), stSEP

	for _, r := range csv {
		if r > '\x7e' || r != '\x09' && r < '\x20' {
			continue // all non-printable ASCII runes dropped
		}

		switch st {
		case stSEP:
			switch r {
			case sep: // separator state
				sl = append(sl, uint16(len(buf)))
			case '"':
				st = stENCL
			case ' ':
				st = stPFX
			default:
				buf, st = append(buf, byte(r)), stIFX
			}
		case stENCL: // double-quote enclosure state (ingests until closing double-quote)
			switch r {
			case '"':
				st = stESC
			default:
				buf = append(buf, byte(r))
			}
		case stESC: // double-quote single-rune escape state (any rune but separator escaped)
			switch r {
			case sep:
				sl, st = append(sl, uint16(len(buf))), stSEP
			default:
				buf, st = append(buf, byte(r)), stENCL
			}
		case stPFX: // unenclosed prefix state (leading blanks skipped)
			switch r {
			case sep:
				sl, st = append(sl, uint16(len(buf))), stSEP
			case ' ':
			default:
				buf, st = append(buf, byte(r)), stIFX
			}
		case stIFX: // unenclosed infix state (ingests until blank/separator)
			switch r {
			case sep:
				sl, st = append(sl, uint16(len(buf))), stSEP
			case ' ':
				buf, slen, st = append(buf, byte(r)), uint16(len(buf)), stSFX
			default:
				buf = append(buf, byte(r))
			}
		case stSFX: // unenclosed suffix state (final blanks deleted)
			switch r {
			case sep:
				sl, buf, st = append(sl, uint16(slen)), buf[:slen], stSEP
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
	return buf, append(sl, uint16(len(buf)))
}

// SplitCSV returns blank-trimmed fields in "csv" split by "sep", using a safe but tolerant
// implementation of RFC 4180
func SplitCSV(csv string, sep rune) []string {
	buf, sl := SliceCSV(csv, sep, 1+len(csv)/4)
	fields := make([]string, 0, len(sl))
	for i := 1; i < len(sl); i++ {
		fields = append(fields, string(buf[sl[i-1]:sl[i]]))
	}
	return fields
}
