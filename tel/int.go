package tel

type (
	rateGrp struct {
		Rate float32
		P    []string
	}
	pRate map[string]float32

	ccInfo struct {
		// ITU source: T-SP-E.164D-11-2011-PDF-E.pdf
		// avoid 3 special characters in country names: ,/"
		Geo     string
		CCn     string
		ISO3166 string
		Pl      int
	}

	spIDs struct {
		Name  string
		Alias []string
	}

	geoCode uint8
)

const (
	gcNIL geoCode = iota
	gcUS48
	gcAKHI
	gcUST
	gcCAN
	gcCAR
	gcNATF
	gcAFR
	gcEUR
	gcLAM
	gcAPAC
	gcRUS
	gcMEA
	gcGLOB
	gcUNUSED1
	gcUNUSED2
)

var (
	geoName = map[geoCode]string{
		gcUS48: "us48",
		gcAKHI: "akhi",
		gcUST:  "ust",
		gcCAN:  "can",
		gcCAR:  "car",
		gcNATF: "natf",
		gcAFR:  "afr",
		gcEUR:  "eur",
		gcLAM:  "lam",
		gcAPAC: "apac",
		gcRUS:  "rus",
		gcMEA:  "mea",
		gcGLOB: "glob",
	}
	geoEncode = map[string]geoCode{
		"us48": gcUS48,
		"akhi": gcAKHI,
		"ust":  gcUST,
		"can":  gcCAN,
		"car":  gcCAR,
		"natf": gcNATF,
		"afr":  gcAFR,
		"eur":  gcEUR,
		"lam":  gcLAM,
		"apac": gcAPAC,
		"rus":  gcRUS,
		"mea":  gcMEA,
		"glob": gcGLOB,
	}
)

const (
	geoShift = 64 - 4             // E164digest Geo code
	ccShift  = 60 - 2             // E164digest CC length
	ccMask   = 0x3                // E164digest CC length
	pShift   = 58 - 4             // E164digest P length
	pMask    = 0xf                // E164digest P length
	subShift = 54 - 4             // E164digest Sub length
	subMask  = 0xf                // E164digest Sub length
	numMask  = 0x3_ffff_ffff_ffff // E164digest Num E.164 number

	defaultProviders = `{
		"0":	{"Name":"unknown",		"Alias":["","unk","?"]},

		"1":	{"Name":"AT&T",			"Alias":["at&t","ATT","att"]},
		"2":	{"Name":"Bandwidth",	"Alias":["BANDWIDTH","bandwidth","BW","bw","BANDWID"]},
		"3":	{"Name":"Brightlink",	"Alias":["BRIGHTLINK","brightlink","BL","bl","BRIGHTL","BRIGHTLSD"]},
		"4":	{"Name":"BT",			"Alias":["bt","British Telecom","britteluk"]},
		"5":	{"Name":"Global Crossing","Alias":["GLOBAL CROSSING","GX","gx"]},
		"6":	{"Name":"IDT",			"Alias":["idt"]},
		"7":	{"Name":"Intelepeer",	"Alias":["INTELEPEER","IP","ip","IPEER"]},
		"8":	{"Name":"Inteliquent",	"Alias":["INTELIQUENT","inteliquent","IQ","iq","NT","nt","INTLQNT"]},
		"9":	{"Name":"Level 3",		"Alias":["LEVEL 3","level 3","LEVEL3","level3","L3","l3"]},
		"10":	{"Name":"NuWave",		"Alias":["NUWAVE","nuwave","NW","nw"]},
		"11":	{"Name":"Tata",			"Alias":["TATA","tata","TA","ta"]},
		"12":	{"Name":"Verizon",		"Alias":["VERIZON","verizon","VZ","vz"]},
		"13":	{"Name":"Voxbone",		"Alias":["VOXBONE","voxbone","VB","vb"]},

		"254":	{"Name":"customer",		"Alias":["BYOC"]},
		"255":	{"Name":"internal",		"Alias":["Aspect","ASPECT","ASP","asp","Voxeo","VOXEO","VX","vx","AUDIOC"]}
	}`

	defaultLocations = `{
		"0":	{"Name":"LAS",			"Alias":["las","","SBC20","SBC21"]},
		"1":	{"Name":"ASH",			"Alias":["ash","SBC60","SBC61"]},
		"2":	{"Name":"LHR",			"Alias":["lhr","SBC40"]},
		"3":	{"Name":"FRA",			"Alias":["fra","SBC50"]},

		"16":	{"Name":"LGW",			"Alias":["lgw","UKSSPRD2A1RBBN","UKSSPRD2B1RBBN","UKSSPRD2C1RBBN"]},

		"254":	{"Name":"AWS lab",		"Alias":["AWS LAB","SSD1A1RBBN","SSD1A2RBBN","SSD1B1RBBN","SSD1B2RBBN","SSD1D1RBBN","SSD1D2RBBN"]},
		"255":	{"Name":"lab",			"Alias":["LAB","SBC1"]}
	}`
)

// xInfo method on Decoder (internal) ...
func (d *Decoder) xInfo(n string, cc string) (i *ccInfo) {
	// TODO: expanded validation/decoding rules (including more precise P/Sub partitioning)
	switch i = d.ccI[cc]; cc {
	case "1":
		// NANPA exceptions
	case "7":
		// Russia/Kazakhstan exceptions
	default:
	}
	return
}

// set method on E164full (internal) ...
func (tn *E164full) set(n string, cc string, i *ccInfo) {
	if i != nil {
		tn.Num, tn.CC = n, cc
		tn.Geo = i.Geo
		tn.CCn = i.CCn
		tn.ISO3166 = i.ISO3166
		if so := len(cc) + i.Pl; len(n) > so {
			tn.P, tn.Sub = n[len(cc):so], n[so:]
		} else {
			tn.P, tn.Sub = "", ""
		}
	} else {
		tn.Num, tn.CC, tn.Geo, tn.CCn, tn.ISO3166, tn.P, tn.Sub = "", "", "", "", "", "", ""
	}
	// tn.CCx, tn.Pn = "", "" // not implemented
}
