package tel

type (
	rateGrp struct {
		Rate float32
		Pre  []string
	}
	ccRate map[string][]rateGrp

	rateMap map[string]float32

	decodeItem struct {
		// ITU source: T-SP-E.164D-11-2011-PDF-E.pdf
		// avoid 3 special characters in country names: ,/'
		Geo     string
		CN      string
		ISO3166 string
		AL      int
	}
	ccDecoder map[string]decodeItem
)

const (
	defaultRates = `{
		"1":[	{"Rate": 0.001, "Pre": ["402"]}],
		"44":[	{"Rate": 0.042, "Pre": ["723","741","722"]},
				{"Rate": 0.039, "Pre": ["760","788"]}]
	}`

	defaultDecoder = `{
		"1":	{"Geo":"nanp",	"ISO3166":"",	"AL":3,	"CN":"North America"},
		"44":	{"Geo":"eur",	"ISO3166":"UK",	"AL":3,	"CN":"United Kingdom"}
	}`
)
