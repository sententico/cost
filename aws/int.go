package aws

type (
	rateInfo struct {
		Region string  `json:"Rg"`
		Typ    string  `json:"T"`
		Plat   string  `json:"P"`
		Terms  string  `json:"Tm"`
		Rate   float32 `json:"R"`
		Core   float32 `json:"C"`
		ECU    string  `json:"ECU"`
		Clock  string  `json:"Cl"`
		Proc   string  `json:"Pr"`
		Feat   string  `json:"Fe"`
		Mem    string  `json:"M"`
		Sto    string  `json:"St"`
		EBS    string  `json:"EBS"`
		Net    string  `json:"Nw"`
	}
)

const (
	defaultRates = `[
		{"Rg":"us-east-1", "T":"c5n.18xlarge", "P":"Lin", "Tm":"OD", "R":3.888},
		{"Rg":"us-east-1", "T":"c5n.18xlarge", "P":"Lin", "Tm":"RI1ns", "R":2.449}
	]`
)
