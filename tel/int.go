package tel

type (
	resRate struct {
		Pre  []string
		Rate float32
	}
	prefixGroup struct {
		Group []resRate
	}
	rateRes struct {
		CC map[string]prefixGroup
	}

	prefixRate struct {
		pre map[string]float32
	}
)

const defaultRates = `{"CC": {
	"1": {"Group":[
	   {"Pre": ["402"],
		"Rate": 0.0001}
	   ]},
	"44": {"Group":[
	   {"Pre": ["723","741"],
		"Rate":	0.042},
	   {"Pre": ["766","75"],
		"Rate":	0.039}
	   ]}
	}}`
