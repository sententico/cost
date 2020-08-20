package tel

type (
	rateGrp struct {
		Rate float32
		Pre  []string
	}
	rateRes map[string][]rateGrp

	prefixMap map[string]float32
)

const defaultRates = `{
	"1":[	{"Rate": 0.001, "Pre": ["402"]}],
	"44":[	{"Rate": 0.042, "Pre": ["723","741","722"]},
			{"Rate": 0.039, "Pre": ["760","788"]}]
}`
