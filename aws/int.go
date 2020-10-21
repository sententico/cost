package aws

type (
	rateInfo struct {
		Region string
		Typ    string
		Plat   string
		Terms  string
		Core   float32
		ECU    string
		Clock  string
		Proc   string
		Feat   string
		Mem    string
		Sto    string
		EBS    string
		Net    string
		Rate   float32
	}
)

const (
	defaultRates = `[
		{"Region":"us-east-1", "Typ":"c5n.18xlarge", "Plat":"Lin", "Terms":"OD", "Rate":3.888},
		{"Region":"us-east-1", "Typ":"c5n.18xlarge", "Plat":"Lin", "Terms":"RI1ns", "Rate":2.449}
	]`
)
