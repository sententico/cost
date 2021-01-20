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
		P       []string
		Sub     []*ccInfo

		subI map[string]*ccInfo
	}

	nameGrp struct {
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
	geoDecode, geoEncode = map[geoCode]string{
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
	}, map[string]geoCode{
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
	numShift = 64 - 50 // E164digest Num E.164 number
	ccShift  = 14 - 2  // E164digest CC length
	ccMask   = 0x3     // E164digest CC length
	pShift   = 12 - 4  // E164digest P length
	pMask    = 0xf     // E164digest P length
	subShift = 8 - 4   // E164digest Sub length
	subMask  = 0xf     // E164digest Sub length
	geoMask  = 0xf     // E164digest Geo code

	// requires maintenance updates (last Sep20)
	defaultProviders = `{
		"0":	{"Name":"unknown",		"Alias":["","unk","?"]},

		"1":	{"Name":"AT&T",			"Alias":["at&t","ATT","att"]},
		"2":	{"Name":"Bandwidth",	"Alias":["BANDWIDTH","bandwidth","BW","bw","BANDWID","BAND"]},
		"3":	{"Name":"Brightlink",	"Alias":["BRIGHTLINK","brightlink","BL","bl","BRIGHTL","BRIGHTLSD"]},
		"4":	{"Name":"BT",			"Alias":["bt","British Telecom","britteluk","BTTC"]},
		"5":	{"Name":"Global Crossing","Alias":["GLOBAL CROSSING","GX","gx"]},
		"6":	{"Name":"IDT",			"Alias":["idt","IDTTC"]},
		"7":	{"Name":"Intelepeer",	"Alias":["INTELEPEER","IP","ip","IPEER"]},
		"8":	{"Name":"Inteliquent",	"Alias":["INTELIQUENT","inteliquent","IQ","iq","NT","nt","INTLQNT"]},
		"9":	{"Name":"Level 3",		"Alias":["LEVEL 3","level 3","LEVEL3","level3","L3","l3"]},
		"10":	{"Name":"NuWave",		"Alias":["NUWAVE","nuwave","NW","nw","ATTNUWAVE","VZNUWAVE"]},
		"11":	{"Name":"Tata",			"Alias":["TATA","tata","TA","ta"]},
		"12":	{"Name":"Verizon",		"Alias":["VERIZON","verizon","VZ","vz"]},
		"13":	{"Name":"Voxbone",		"Alias":["VOXBONE","voxbone","VB","vb"]},

		"61":	{"Name":"fallback",		"Alias":["NORATES"]},
		"62":	{"Name":"customer",		"Alias":["Customer","CUSTOMER","Cust","CUST","cust","BYOC","byoc","PBXC","pbxc"]},
		"63":	{"Name":"internal",		"Alias":["Aspect","ASPECT","ASP","asp","Voxeo","VOXEO","VX","vx","AUDIOC"]}
	}`

	// requires maintenance updates (last Sep20)
	defaultLocations = `{
		"0":	{"Name":"ASH",			"Alias":["#US primary",	"ash","",	"SBC60","SBC61"]},
		"1":	{"Name":"LAS",			"Alias":["#US standby",	"las",		"SBC20","SBC21"]},
		"2":	{"Name":"FRA",			"Alias":["#EU primary",	"fra",		"SBC50"]},
		"3":	{"Name":"LHR",			"Alias":["#EU standby",	"lhr",		"SBC40"]},

		"7":	{"Name":"LGW",			"Alias":["#UK primary",	"lgw",		"UKSSPRD2A1RBBN","UKSSPRD2B1RBBN","UKSSPRD2C1RBBN"]},

		"62":	{"Name":"AWS lab",		"Alias":["AWS LAB",	"soft lab",		"SSD1A1RBBN","SSD1A2RBBN","SSD1B1RBBN","SSD1B2RBBN","SSD1D1RBBN","SSD1D2RBBN"]},
		"63":	{"Name":"lab",			"Alias":["LAB",	"hard lab",			"SBC1"]}
	}`

	// requires maintenance updates (last Oct20)
	defaultEncodings = `{
		"1":	{"Geo":"nanpa",	"ISO3166":"XC",	"Pl":3,	"CCn":"North America", "Sub":[
				{"Geo":"akhi",	"ISO3166":"US",	"Pl":3,	"CCn":"United States of America",
								"P":["808","907"]},
				{"Geo":"natf",	"ISO3166":"XC",	"Pl":3, "CCn":"North America",
								"P":["800","822","833","844","855","866","877","888",
									 "880","881","882","883","884","885","886","887","889"]},
				{"Geo":"us48",	"ISO3166":"US",	"Pl":3, "CCn":"United States of America",
								"P":[""]},
				{"Geo":"ust",	"ISO3166":"VI",	"Pl":3,	"CCn":"United States Virgin Islands",
								"P":["340"]},
				{"Geo":"ust",	"ISO3166":"MP",	"Pl":3,	"CCn":"Northern Mariana Islands",
								"P":["670"]},
				{"Geo":"ust",	"ISO3166":"GU",	"Pl":3,	"CCn":"Guam",
								"P":["671"]},
				{"Geo":"ust",	"ISO3166":"AS",	"Pl":3,	"CCn":"American Samoa",
								"P":["684"]},
				{"Geo":"ust",	"ISO3166":"PR",	"Pl":3,	"CCn":"Puerto Rico",
								"P":["787","939"]},

				{"Geo":"can",	"ISO3166":"CA",	"Pl":3,	"CCn":"Canada",
								"P":["204","226","236","249","250","263","289",
									 "306","343","354","365","367","368","382","387",
									 "403","416","418","428","431","437","438","450","468","474",
									 "506","514","519","548","579","581","584","587",
									 "600","604","613","639","647","672","683",
									 "705","709","742","753","778","780","782",
									 "807","819","825","867","873","879",
									 "902","905"]},
				{"Geo":"car",	"ISO3166":"BS",	"Pl":3,	"CCn":"Bahamas",
								"P":["242"]},
				{"Geo":"car",	"ISO3166":"BB",	"Pl":3,	"CCn":"Barbados",
								"P":["246"]},
				{"Geo":"car",	"ISO3166":"AI",	"Pl":3,	"CCn":"Anguilla",
								"P":["264"]},
				{"Geo":"car",	"ISO3166":"AI",	"Pl":3,	"CCn":"Antigua & Barbuda",
								"P":["268"]},
				{"Geo":"car",   "ISO3166":"VG",	"Pl":3,	"CCn":"British Virgin Islands",
								"P":["284"]},
				{"Geo":"car",   "ISO3166":"KY",	"Pl":3,	"CCn":"Cayman Islands",
								"P":["345"]},
				{"Geo":"car",   "ISO3166":"BM",	"Pl":3,	"CCn":"Bermuda",
								"P":["441"]},
				{"Geo":"car",   "ISO3166":"GD",	"Pl":3,	"CCn":"Grenada",
								"P":["473"]},
				{"Geo":"car",   "ISO3166":"TC",	"Pl":3,	"CCn":"Turks & Caicos Islands",
								"P":["649"]},
				{"Geo":"car",   "ISO3166":"MS",	"Pl":3,	"CCn":"Montserrat",
								"P":["664"]},
				{"Geo":"car",   "ISO3166":"SX",	"Pl":3,	"CCn":"Sint Maarten",
								"P":["721"]},
				{"Geo":"car",   "ISO3166":"LC",	"Pl":3,	"CCn":"Saint Lucia",
								"P":["758"]},
				{"Geo":"car",   "ISO3166":"DM",	"Pl":3,	"CCn":"Dominica",
								"P":["767"]},
				{"Geo":"car",   "ISO3166":"VC",	"Pl":3,	"CCn":"Saint Vincent & Grenadines",
								"P":["784"]},
				{"Geo":"car",   "ISO3166":"DO",	"Pl":3,	"CCn":"Dominican Republic",
								"P":["809","829","849"]},
				{"Geo":"car",   "ISO3166":"TT",	"Pl":3,	"CCn":"Trinidad & Tobago",
								"P":["868"]},
				{"Geo":"car",   "ISO3166":"KN",	"Pl":3,	"CCn":"Saint Kitts & Nevis",
								"P":["869"]},
				{"Geo":"car",   "ISO3166":"JM",	"Pl":3,	"CCn":"Jamaica",
								"P":["658","876"]} ]},

		"20":	{"Geo":"afr",	"ISO3166":"EG",	"Pl":2,	"CCn":"Egypt"},
		"211":	{"Geo":"afr",	"ISO3166":"SS",	"Pl":3,	"CCn":"South Sudan"},
		"212":	{"Geo":"afr",	"ISO3166":"MA",	"Pl":3,	"CCn":"Morocco"},
		"213":	{"Geo":"afr",	"ISO3166":"DZ",	"Pl":3,	"CCn":"Algeria"},
		"216":	{"Geo":"afr",	"ISO3166":"TN",	"Pl":3,	"CCn":"Tunisia"},
		"218":	{"Geo":"afr",	"ISO3166":"LY",	"Pl":3,	"CCn":"Libya"},
		"220":	{"Geo":"afr",	"ISO3166":"GM",	"Pl":3,	"CCn":"Gambia"},
		"221":	{"Geo":"afr",	"ISO3166":"SN",	"Pl":3,	"CCn":"Senegal"},
		"222":	{"Geo":"afr",	"ISO3166":"MR",	"Pl":3,	"CCn":"Mauritania"},
		"223":	{"Geo":"afr",	"ISO3166":"ML",	"Pl":3,	"CCn":"Mali"},
		"224":	{"Geo":"afr",	"ISO3166":"GN",	"Pl":3,	"CCn":"Guinea"},
		"225":	{"Geo":"afr",	"ISO3166":"CI",	"Pl":3,	"CCn":"Ivory Coast"},
		"226":	{"Geo":"afr",	"ISO3166":"BF",	"Pl":3,	"CCn":"Burkina Faso"},
		"227":	{"Geo":"afr",	"ISO3166":"NE",	"Pl":3,	"CCn":"Niger"},
		"228":	{"Geo":"afr",	"ISO3166":"TG",	"Pl":3,	"CCn":"Togo"},
		"229":	{"Geo":"afr",	"ISO3166":"BJ",	"Pl":3,	"CCn":"Benin"},
		"230":	{"Geo":"afr",	"ISO3166":"MU",	"Pl":3,	"CCn":"Mauritius"},
		"231":	{"Geo":"afr",	"ISO3166":"LR",	"Pl":3,	"CCn":"Liberia"},
		"232":	{"Geo":"afr",	"ISO3166":"SL",	"Pl":3,	"CCn":"Sierra Leone"},
		"233":	{"Geo":"afr",	"ISO3166":"GH",	"Pl":3,	"CCn":"Ghana"},
		"234":	{"Geo":"afr",	"ISO3166":"NG",	"Pl":3,	"CCn":"Nigeria"},
		"235":	{"Geo":"afr",	"ISO3166":"TD",	"Pl":3,	"CCn":"Chad"},
		"236":	{"Geo":"afr",	"ISO3166":"CF",	"Pl":3,	"CCn":"Central African Republic"},
		"237":	{"Geo":"afr",	"ISO3166":"CM",	"Pl":3,	"CCn":"Cameroon"},
		"238":	{"Geo":"afr",	"ISO3166":"CV",	"Pl":3,	"CCn":"Cape Verde"},
		"239":	{"Geo":"afr",	"ISO3166":"ST",	"Pl":3,	"CCn":"Sao Tome & Principe"},
		"240":	{"Geo":"afr",	"ISO3166":"GQ",	"Pl":3,	"CCn":"Equatorial Guinea"},
		"241":	{"Geo":"afr",	"ISO3166":"GA",	"Pl":3,	"CCn":"Gabon"},
		"242":	{"Geo":"afr",	"ISO3166":"CG",	"Pl":3,	"CCn":"Congo"},
		"243":	{"Geo":"afr",	"ISO3166":"CD",	"Pl":3,	"CCn":"Congo DR"},
		"244":	{"Geo":"afr",	"ISO3166":"AO",	"Pl":3,	"CCn":"Angola"},
		"245":	{"Geo":"afr",	"ISO3166":"GW",	"Pl":3,	"CCn":"Guinea-Bissau"},
		"246":	{"Geo":"afr",	"ISO3166":"DG",	"Pl":3,	"CCn":"Diego Garcia"},
		"247":	{"Geo":"afr",	"ISO3166":"SH",	"Pl":3,	"CCn":"Ascension"},
		"248":	{"Geo":"afr",	"ISO3166":"SC",	"Pl":3,	"CCn":"Seychelles"},
		"249":	{"Geo":"afr",	"ISO3166":"SD",	"Pl":3,	"CCn":"Sudan"},
		"250":	{"Geo":"afr",	"ISO3166":"RW",	"Pl":3,	"CCn":"Rwanda"},
		"251":	{"Geo":"afr",	"ISO3166":"ET",	"Pl":3,	"CCn":"Ethiopia"},
		"252":	{"Geo":"afr",	"ISO3166":"SO",	"Pl":3,	"CCn":"Somalia"},
		"253":	{"Geo":"afr",	"ISO3166":"DJ",	"Pl":3,	"CCn":"Djibouti"},
		"254":	{"Geo":"afr",	"ISO3166":"KE",	"Pl":3,	"CCn":"Kenya"},
		"255":	{"Geo":"afr",	"ISO3166":"TZ",	"Pl":3,	"CCn":"Tanzania"},
		"256":	{"Geo":"afr",	"ISO3166":"UG",	"Pl":3,	"CCn":"Uganda"},
		"257":	{"Geo":"afr",	"ISO3166":"BI",	"Pl":3,	"CCn":"Burundi"},
		"258":	{"Geo":"afr",	"ISO3166":"MZ",	"Pl":3,	"CCn":"Mozambique"},
		"260":	{"Geo":"afr",	"ISO3166":"ZM",	"Pl":3,	"CCn":"Zambia"},
		"261":	{"Geo":"afr",	"ISO3166":"MG",	"Pl":3,	"CCn":"Madagascar"},
		"262":	{"Geo":"afr",	"ISO3166":"RE",	"Pl":3,	"CCn":"Reunion"},
		"263":	{"Geo":"afr",	"ISO3166":"ZW",	"Pl":3,	"CCn":"Zimbabwe"},
		"264":	{"Geo":"afr",	"ISO3166":"NA",	"Pl":3,	"CCn":"Namibia"},
		"265":	{"Geo":"afr",	"ISO3166":"MW",	"Pl":3,	"CCn":"Malawi"},
		"266":	{"Geo":"afr",	"ISO3166":"LS",	"Pl":3,	"CCn":"Lesotho"},
		"267":	{"Geo":"afr",	"ISO3166":"BW",	"Pl":3,	"CCn":"Botswana"},
		"268":	{"Geo":"afr",	"ISO3166":"SZ",	"Pl":3,	"CCn":"Swaziland"},
		"269":	{"Geo":"afr",	"ISO3166":"KM",	"Pl":3,	"CCn":"Comoros"},
		"27":	{"Geo":"afr",	"ISO3166":"ZA",	"Pl":2,	"CCn":"South Africa"},
		"290":	{"Geo":"afr",	"ISO3166":"SH",	"Pl":3,	"CCn":"Saint Helena & Tristan da Cunha"},
		"291":	{"Geo":"afr",	"ISO3166":"ER",	"Pl":3,	"CCn":"Eritrea"},
		"297":	{"Geo":"lam",	"ISO3166":"AW",	"Pl":3,	"CCn":"Aruba"},
		"298":	{"Geo":"eur",	"ISO3166":"FO",	"Pl":3,	"CCn":"Faroe Islands"},
		"299":	{"Geo":"eur",	"ISO3166":"GL",	"Pl":3,	"CCn":"Greenland"},

		"30":	{"Geo":"eur",	"ISO3166":"GR",	"Pl":3,	"CCn":"Greece"},
		"31":	{"Geo":"eur",	"ISO3166":"NL",	"Pl":2,	"CCn":"Netherlands"},
		"32":	{"Geo":"eur",	"ISO3166":"BE",	"Pl":2,	"CCn":"Belgium"},
		"33":	{"Geo":"eur",	"ISO3166":"FR",	"Pl":1,	"CCn":"France"},
		"34":	{"Geo":"eur",	"ISO3166":"ES",	"Pl":3,	"CCn":"Spain"},
		"350":	{"Geo":"eur",	"ISO3166":"GI",	"Pl":3,	"CCn":"Gibraltar"},
		"351":	{"Geo":"eur",	"ISO3166":"PT",	"Pl":3,	"CCn":"Portugal"},
		"352":	{"Geo":"eur",	"ISO3166":"LU",	"Pl":3,	"CCn":"Luxembourg"},
		"353":	{"Geo":"eur",	"ISO3166":"IE",	"Pl":2,	"CCn":"Ireland"},
		"354":	{"Geo":"eur",	"ISO3166":"IS",	"Pl":3,	"CCn":"Iceland"},
		"355":	{"Geo":"eur",	"ISO3166":"AL",	"Pl":3,	"CCn":"Albania"},
		"356":	{"Geo":"eur",	"ISO3166":"MT",	"Pl":3,	"CCn":"Malta"},
		"357":	{"Geo":"eur",	"ISO3166":"CY",	"Pl":3,	"CCn":"Cyprus"},
		"358":	{"Geo":"eur",	"ISO3166":"FI",	"Pl":3,	"CCn":"Finland"},
		"359":	{"Geo":"eur",	"ISO3166":"BG",	"Pl":3,	"CCn":"Bulgaria"},
		"36":	{"Geo":"eur",	"ISO3166":"HU",	"Pl":2,	"CCn":"Hungary"},
		"370":	{"Geo":"eur",	"ISO3166":"LT",	"Pl":3,	"CCn":"Lithuania"},
		"371":	{"Geo":"eur",	"ISO3166":"LV",	"Pl":3,	"CCn":"Latvia"},
		"372":	{"Geo":"eur",	"ISO3166":"EE",	"Pl":3,	"CCn":"Estonia"},
		"373":	{"Geo":"eur",	"ISO3166":"MD",	"Pl":3,	"CCn":"Moldova"},
		"374":	{"Geo":"eur",	"ISO3166":"AM",	"Pl":3,	"CCn":"Armenia"},
		"375":	{"Geo":"eur",	"ISO3166":"BY",	"Pl":3,	"CCn":"Belarus"},
		"376":	{"Geo":"eur",	"ISO3166":"AD",	"Pl":3,	"CCn":"Andorra"},
		"377":	{"Geo":"eur",	"ISO3166":"MC",	"Pl":3,	"CCn":"Monaco"},
		"378":	{"Geo":"eur",	"ISO3166":"SM",	"Pl":3,	"CCn":"San Marino"},
		"379":	{"Geo":"eur",	"ISO3166":"VA",	"Pl":0,	"CCn":"Holy See"},
		"380":	{"Geo":"eur",	"ISO3166":"UA",	"Pl":3,	"CCn":"Ukraine"},
		"381":	{"Geo":"eur",	"ISO3166":"RS",	"Pl":3,	"CCn":"Serbia"},
		"382":	{"Geo":"eur",	"ISO3166":"ME",	"Pl":3,	"CCn":"Montenegro"},
		"385":	{"Geo":"eur",	"ISO3166":"HR",	"Pl":3,	"CCn":"Croatia"},
		"386":	{"Geo":"eur",	"ISO3166":"SI",	"Pl":2,	"CCn":"Slovenia"},
		"387":	{"Geo":"eur",	"ISO3166":"BA",	"Pl":3,	"CCn":"Bosnia & Herzegovina"},
		"388":	{"Geo":"glob",	"ISO3166":"XC",	"Pl":0,	"CCn":"EU shared"},
		"389":	{"Geo":"eur",	"ISO3166":"MK",	"Pl":3,	"CCn":"Macedonia"},
		"39":	{"Geo":"eur",	"ISO3166":"IT",	"Pl":3,	"CCn":"Italy"},

		"40":	{"Geo":"eur",	"ISO3166":"RO",	"Pl":3,	"CCn":"Romania"},
		"41":	{"Geo":"eur",	"ISO3166":"CH",	"Pl":2,	"CCn":"Switzerland"},
		"420":	{"Geo":"eur",	"ISO3166":"CZ",	"Pl":3,	"CCn":"Czech Republic"},
		"421":	{"Geo":"eur",	"ISO3166":"SK",	"Pl":3,	"CCn":"Slovakia"},
		"423":	{"Geo":"eur",	"ISO3166":"LI",	"Pl":1,	"CCn":"Liechtenstein"},
		"43":	{"Geo":"eur",	"ISO3166":"AT",	"Pl":3,	"CCn":"Austria"},
		"44":	{"Geo":"eur",	"ISO3166":"GB",	"Pl":3,	"CCn":"United Kingdom"},
		"45":	{"Geo":"eur",	"ISO3166":"DK",	"Pl":2,	"CCn":"Denmark"},
		"46":	{"Geo":"eur",	"ISO3166":"SE",	"Pl":2,	"CCn":"Sweden"},
		"47":	{"Geo":"eur",	"ISO3166":"NO",	"Pl":2,	"CCn":"Norway"},
		"48":	{"Geo":"eur",	"ISO3166":"PL",	"Pl":2,	"CCn":"Poland"},
		"49":	{"Geo":"eur",	"ISO3166":"DE",	"Pl":3,	"CCn":"Germany"},

		"500":	{"Geo":"lam",	"ISO3166":"FK",	"Pl":3,	"CCn":"Falkland Islands"},
		"501":	{"Geo":"lam",	"ISO3166":"BZ",	"Pl":3,	"CCn":"Belize"},
		"502":	{"Geo":"lam",	"ISO3166":"GT",	"Pl":3,	"CCn":"Guatemala"},
		"503":	{"Geo":"lam",	"ISO3166":"SV",	"Pl":3,	"CCn":"El Salvador"},
		"504":	{"Geo":"lam",	"ISO3166":"HN",	"Pl":3,	"CCn":"Honduras"},
		"505":	{"Geo":"lam",	"ISO3166":"NI",	"Pl":3,	"CCn":"Nicaragua"},
		"506":	{"Geo":"lam",	"ISO3166":"CR",	"Pl":3,	"CCn":"Costa Rica"},
		"507":	{"Geo":"lam",	"ISO3166":"PA",	"Pl":3,	"CCn":"Panama"},
		"508":	{"Geo":"lam",	"ISO3166":"PM",	"Pl":3,	"CCn":"Saint Pierre & Miquelon"},
		"509":	{"Geo":"lam",	"ISO3166":"HT",	"Pl":3,	"CCn":"Haiti"},
		"51":	{"Geo":"lam",	"ISO3166":"PE",	"Pl":3,	"CCn":"Peru"},
		"52":	{"Geo":"lam",	"ISO3166":"MX",	"Pl":3,	"CCn":"Mexico"},
		"53":	{"Geo":"lam",	"ISO3166":"CU",	"Pl":1,	"CCn":"Cuba"},
		"54":	{"Geo":"lam",	"ISO3166":"AR",	"Pl":3,	"CCn":"Argentina"},
		"55":	{"Geo":"lam",	"ISO3166":"BR",	"Pl":2,	"CCn":"Brazil"},
		"56":	{"Geo":"lam",	"ISO3166":"CL",	"Pl":0,	"CCn":"Chile"},
		"57":	{"Geo":"lam",	"ISO3166":"CO",	"Pl":1,	"CCn":"Colombia"},
		"58":	{"Geo":"lam",	"ISO3166":"VE",	"Pl":3,	"CCn":"Venezuela"},
		"590":	{"Geo":"lam",	"ISO3166":"GP",	"Pl":3,	"CCn":"Guadeloupe"},
		"591":	{"Geo":"lam",	"ISO3166":"BO",	"Pl":3,	"CCn":"Bolivia"},
		"592":	{"Geo":"lam",	"ISO3166":"GY",	"Pl":3,	"CCn":"Guyana"},
		"593":	{"Geo":"lam",	"ISO3166":"EC",	"Pl":3,	"CCn":"Ecuador"},
		"594":	{"Geo":"lam",	"ISO3166":"GF",	"Pl":3,	"CCn":"French Guiana"},
		"595":	{"Geo":"lam",	"ISO3166":"PY",	"Pl":3,	"CCn":"Paraguay"},
		"596":	{"Geo":"lam",	"ISO3166":"MQ",	"Pl":3,	"CCn":"Martinique"},
		"597":	{"Geo":"lam",	"ISO3166":"SR",	"Pl":3,	"CCn":"Suriname"},
		"598":	{"Geo":"lam",	"ISO3166":"UY",	"Pl":3,	"CCn":"Uruguay"},
		"599":	{"Geo":"lam",	"ISO3166":"CW",	"Pl":3,	"CCn":"Caribbean Netherlands"},

		"60":	{"Geo":"apac",	"ISO3166":"MY",	"Pl":2,	"CCn":"Malaysia"},
		"61":	{"Geo":"apac",	"ISO3166":"AU",	"Pl":3,	"CCn":"Australia"},
		"62":	{"Geo":"apac",	"ISO3166":"ID",	"Pl":3,	"CCn":"Indonesia"},
		"63":	{"Geo":"apac",	"ISO3166":"PH",	"Pl":2,	"CCn":"Philippines"},
		"64":	{"Geo":"apac",	"ISO3166":"NZ",	"Pl":3,	"CCn":"New Zealand"},
		"65":	{"Geo":"apac",	"ISO3166":"SG",	"Pl":1,	"CCn":"Singapore"},
		"66":	{"Geo":"apac",	"ISO3166":"TH",	"Pl":2,	"CCn":"Thailand"},
		"670":	{"Geo":"apac",	"ISO3166":"TL",	"Pl":3,	"CCn":"Timor-Leste"},
		"672":	{"Geo":"apac",	"ISO3166":"NF",	"Pl":3,	"CCn":"Australian External Territories"},
		"673":	{"Geo":"apac",	"ISO3166":"BN",	"Pl":3,	"CCn":"Brunei Darussalam"},
		"674":	{"Geo":"apac",	"ISO3166":"NR",	"Pl":3,	"CCn":"Nauru"},
		"675":	{"Geo":"apac",	"ISO3166":"PG",	"Pl":3,	"CCn":"Papua New Guinea"},
		"676":	{"Geo":"apac",	"ISO3166":"TO",	"Pl":3,	"CCn":"Tonga"},
		"677":	{"Geo":"apac",	"ISO3166":"SB",	"Pl":3,	"CCn":"Solomon Islands"},
		"678":	{"Geo":"apac",	"ISO3166":"VU",	"Pl":3,	"CCn":"Vanuatu"},
		"679":	{"Geo":"apac",	"ISO3166":"FJ",	"Pl":3,	"CCn":"Fiji"},
		"680":	{"Geo":"apac",	"ISO3166":"PW",	"Pl":3,	"CCn":"Palau"},
		"681":	{"Geo":"apac",	"ISO3166":"WF",	"Pl":3,	"CCn":"Wallis & Futuna"},
		"682":	{"Geo":"apac",	"ISO3166":"CK",	"Pl":3,	"CCn":"Cook Islands"},
		"683":	{"Geo":"apac",	"ISO3166":"NU",	"Pl":3,	"CCn":"Niue"},
		"685":	{"Geo":"apac",	"ISO3166":"WS",	"Pl":3,	"CCn":"Samoa"},
		"686":	{"Geo":"apac",	"ISO3166":"KI",	"Pl":3,	"CCn":"Kiribati"},
		"687":	{"Geo":"apac",	"ISO3166":"NC",	"Pl":3,	"CCn":"New Caledonia"},
		"688":	{"Geo":"apac",	"ISO3166":"TV",	"Pl":3,	"CCn":"Tuvalu"},
		"689":	{"Geo":"apac",	"ISO3166":"PF",	"Pl":3,	"CCn":"French Polynesia"},
		"690":	{"Geo":"apac",	"ISO3166":"TK",	"Pl":1,	"CCn":"Tokelau"},
		"691":	{"Geo":"apac",	"ISO3166":"FM",	"Pl":3,	"CCn":"Micronesia"},
		"692":	{"Geo":"apac",	"ISO3166":"MH",	"Pl":3,	"CCn":"Marshall Islands"},

		"7":	{"Geo":"rus",	"ISO3166":"XC",	"Pl":1,	"CCn":"Russia & Kazakhstan", "Sub":[
				{"Geo":"rus",	"ISO3166":"KZ",	"Pl":3, "CCn":"Kazakhstan",
								"P":["6","7"]},
				{"Geo":"rus",	"ISO3166":"RU",	"Pl":3, "CCn":"Russia",
								"P":[""]} ]},

		"800":	{"Geo":"glob",	"ISO3166":"XC",	"Pl":0,	"CCn":"global freephone"},
		"808":	{"Geo":"glob",	"ISO3166":"XC",	"Pl":0,	"CCn":"global shared cost"},
		"81":	{"Geo":"apac",	"ISO3166":"JP",	"Pl":2,	"CCn":"Japan"},
		"82":	{"Geo":"apac",	"ISO3166":"KR",	"Pl":2,	"CCn":"Korea"},
		"84":	{"Geo":"apac",	"ISO3166":"VN",	"Pl":2,	"CCn":"Vietnam"},
		"850":	{"Geo":"apac",	"ISO3166":"KP",	"Pl":3,	"CCn":"Korea DPR"},
		"852":	{"Geo":"apac",	"ISO3166":"HK",	"Pl":1,	"CCn":"Hong Kong"},
		"853":	{"Geo":"apac",	"ISO3166":"MO",	"Pl":3,	"CCn":"Macao"},
		"855":	{"Geo":"apac",	"ISO3166":"KH",	"Pl":3,	"CCn":"Cambodia"},
		"856":	{"Geo":"apac",	"ISO3166":"LA",	"Pl":3,	"CCn":"Laos"},
		"86":	{"Geo":"apac",	"ISO3166":"CN",	"Pl":3,	"CCn":"China"},
		"870":	{"Geo":"glob",	"ISO3166":"XC",	"Pl":0,	"CCn":"global Inmarsat"},
		"878":	{"Geo":"glob",	"ISO3166":"XC",	"Pl":2,	"CCn":"global personal numbers"},
		"880":	{"Geo":"apac",	"ISO3166":"BD",	"Pl":3,	"CCn":"Bangladesh"},
		"881":	{"Geo":"glob",	"ISO3166":"XC",	"Pl":1,	"CCn":"global satphone"},
		"882":	{"Geo":"glob",	"ISO3166":"XC",	"Pl":2,	"CCn":"global 882"},
		"883":	{"Geo":"glob",	"ISO3166":"XC",	"Pl":3,	"CCn":"global 883"},
		"886":	{"Geo":"apac",	"ISO3166":"TW",	"Pl":3,	"CCn":"Taiwan"},
		"888":	{"Geo":"glob",	"ISO3166":"XC",	"Pl":0,	"CCn":"global humanitarian affairs"},

		"90":	{"Geo":"mea",	"ISO3166":"TR",	"Pl":3,	"CCn":"Turkey"},
		"91":	{"Geo":"mea",	"ISO3166":"IN",	"Pl":4,	"CCn":"India"},
		"92":	{"Geo":"mea",	"ISO3166":"PK",	"Pl":2,	"CCn":"Pakistan"},
		"93":	{"Geo":"mea",	"ISO3166":"AF",	"Pl":2,	"CCn":"Afghanistan"},
		"94":	{"Geo":"mea",	"ISO3166":"LK",	"Pl":3,	"CCn":"Sri Lanka"},
		"95":	{"Geo":"mea",	"ISO3166":"MM",	"Pl":4,	"CCn":"Myanmar"},
		"960":	{"Geo":"mea",	"ISO3166":"MV",	"Pl":3,	"CCn":"Maldives"},
		"961":	{"Geo":"mea",	"ISO3166":"LB",	"Pl":3,	"CCn":"Lebanon"},
		"962":	{"Geo":"mea",	"ISO3166":"JO",	"Pl":3,	"CCn":"Jordan"},
		"963":	{"Geo":"mea",	"ISO3166":"SY",	"Pl":3,	"CCn":"Syria"},
		"964":	{"Geo":"mea",	"ISO3166":"IQ",	"Pl":3,	"CCn":"Iraq"},
		"965":	{"Geo":"mea",	"ISO3166":"KW",	"Pl":4,	"CCn":"Kuwait"},
		"966":	{"Geo":"mea",	"ISO3166":"SA",	"Pl":3,	"CCn":"Saudi Arabia"},
		"967":	{"Geo":"mea",	"ISO3166":"YE",	"Pl":3,	"CCn":"Yemen"},
		"968":	{"Geo":"mea",	"ISO3166":"OM",	"Pl":3,	"CCn":"Oman"},
		"970":	{"Geo":"mea",	"ISO3166":"PS",	"Pl":3,	"CCn":"Palestine"},
		"971":	{"Geo":"mea",	"ISO3166":"AE",	"Pl":3,	"CCn":"United Arab Emirates"},
		"972":	{"Geo":"mea",	"ISO3166":"IL",	"Pl":1,	"CCn":"Israel"},
		"973":	{"Geo":"mea",	"ISO3166":"BH",	"Pl":3,	"CCn":"Bahrain"},
		"974":	{"Geo":"mea",	"ISO3166":"QA",	"Pl":3,	"CCn":"Qatar"},
		"975":	{"Geo":"mea",	"ISO3166":"BT",	"Pl":3,	"CCn":"Bhutan"},
		"976":	{"Geo":"mea",	"ISO3166":"MN",	"Pl":3,	"CCn":"Mongolia"},
		"977":	{"Geo":"mea",	"ISO3166":"NP",	"Pl":3,	"CCn":"Nepal"},
		"979":	{"Geo":"glob",	"ISO3166":"XC",	"Pl":1,	"CCn":"global premium rate"},
		"98":	{"Geo":"mea",	"ISO3166":"IR",	"Pl":3,	"CCn":"Iran"},
		"991":	{"Geo":"glob",	"ISO3166":"XC",	"Pl":3,	"CCn":"global ITPCS trial"},
		"992":	{"Geo":"mea",	"ISO3166":"TJ",	"Pl":3,	"CCn":"Tajikistan"},
		"993":	{"Geo":"mea",	"ISO3166":"TM",	"Pl":3,	"CCn":"Turkmenistan"},
		"994":	{"Geo":"mea",	"ISO3166":"AZ",	"Pl":3,	"CCn":"Azerbaijan"},
		"995":	{"Geo":"mea",	"ISO3166":"GE",	"Pl":3,	"CCn":"Georgia"},
		"996":	{"Geo":"mea",	"ISO3166":"KG",	"Pl":3,	"CCn":"Kyrgyzstan"},
		"998":	{"Geo":"mea",	"ISO3166":"UZ",	"Pl":3,	"CCn":"Uzbekistan"}
	}`
)

// ccInfo method on Decoder (internal) ...
func (d *Decoder) ccInfo(n string, cc string) (i *ccInfo, p string, s string) {
	var mi *ccInfo
	i, nat := d.ccI[cc], n[len(cc):]
	if i == nil || i.subI == nil || i.Pl >= len(nat) {
	} else if i, mi = i.subI[nat[:i.Pl]], i; i == nil {
		i = mi.subI[""]
	}
	if i == nil || i.Pl >= len(nat) {
		return
	}
	set, err := func(pl int, l ...int) (ok bool) {
		switch len(l) {
		case 1:
			ok = l[0] == len(nat)
		case 2:
			ok = l[0] <= len(nat) && len(nat) <= l[1]
		}
		if !ok {
			if s = ""; len(p) != pl && len(nat) > pl {
				p = nat[:pl]
			}
		} else if len(p) != pl || s == "" {
			p, s = nat[:pl], nat[pl:]
		}
		return
	}, func() { p, s = "", "" }

	switch p, s = nat[:i.Pl], nat[i.Pl:]; cc { // en.wikipedia.org/wiki/List_of_mobile_telephone_prefixes_by_country
	case "1": // NANPA (Jan21) en.wikipedia.org/wiki/North_American_Numbering_Plan nationalnanpa.com/area_codes/
		if len(nat) != 10 ||
			nat[0] == '0' || nat[0] == '1' || nat[1] == '9' || nat[3] == '0' || nat[3] == '1' ||
			nat[:2] == "37" || nat[:2] == "96" ||
			nat[:3] == "555" || nat[:3] == "950" || nat[:3] == "988" ||
			nat[1:3] == "11" || nat[3:8] == "55501" { // excluded Jan21: nat[4:6]=="11"
			err()
		}

	case "20": // Egypt (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Egypt
		switch nat[0] {
		case '1':
			if nat[1] == '3' {
				set(2, 9)
			} else {
				set(2, 10)
			}
		case '2':
			set(1, 9)
		case '3':
			set(1, 8)
		default:
			set(2, 9)
		}
	case "212": // Morocco (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Morocco
		switch nat[0] {
		case '5':
			set(4, 9)
		case '6', '7':
			set(3, 9)
		case '8':
			set(2, 9)
		default:
			err()
		}
	//case "211": // South Sudan (3)
	//case "213": // Algeria (3)
	//case "216": // Tunisia (3)
	//case "218": // Libya (3)
	//case "220": // Gambia (3)
	//case "221": // Senegal (3)
	//case "222": // Mauritania (3)
	//case "223": // Mali (3)
	//case "224": // Guinea (3)
	//case "225": // Ivory Coast (3)
	//case "226": // Burkina Faso (3)
	//case "227": // Niger (3)
	//case "228": // Togo (3)
	//case "229": // Benin (3)
	//case "230": // Mauritius (3)
	//case "231": // Liberia (3)
	//case "232": // Sierra Leone (3)
	//case "233": // Ghana (3)
	//case "234": // Nigeria (3)
	//case "235": // Chad (3)
	//case "236": // Central African Republic (3)
	//case "237": // Cameroon (3)
	//case "238": // Cape Verde (3)
	//case "239": // Sao Tome & Principe (3)
	//case "240": // Equatorial Guinea (3)
	//case "241": // Gabon (3)
	//case "242": // Congo (3)
	//case "243": // Congo DR (3)
	//case "244": // Angola (3)
	//case "245": // Guinea-Bissau (3)
	//case "246": // Diego Garcia (3)
	//case "247": // Ascension (3)
	//case "248": // Seychelles (3)
	//case "249": // Sudan (3)
	//case "250": // Rwanda (3)
	//case "251": // Ethiopia (3)
	//case "252": // Somalia (3)
	//case "253": // Djibouti (3)
	//case "254": // Kenya (3)
	//case "255": // Tanzania (3)
	//case "256": // Uganda (3)
	//case "257": // Burundi (3)
	//case "258": // Mozambique (3)
	//case "260": // Zambia (3)
	//case "261": // Madagascar (3)
	//case "262": // Reunion (3)
	//case "263": // Zimbabwe (3)
	//case "264": // Namibia (3)
	//case "265": // Malawi (3)
	//case "266": // Lesotho (3)
	//case "267": // Botswana (3)
	//case "268": // Swaziland (3)
	//case "269": // Comoros (3)
	case "27": // South Africa (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_South_Africa
		switch nat[0] {
		case '1', '2', '3', '4', '5', '7', '8', '9':
			set(2, 9)
		case '6':
			set(3, 9)
		default:
			err()
		}
	//case "290": // Saint Helena & Tristan da Cunha (3)
	//case "291": // Eritrea (3)
	//case "297": // Aruba (3)
	//case "298": // Faroe Islands (3)
	//case "299": // Greenland (3)

	case "30": // Greece (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Greece
		switch nat[0] {
		case '2':
			if nat[1] == '1' {
				set(2, 10)
			} else if nat[2] == '1' {
				set(3, 10)
			} else {
				set(4, 10)
			}
		case '5', '6', '8', '9':
			set(3, 10)
		default:
			err()
		}
	case "31": // Netherlands (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_the_Netherlands
		switch nat[0] {
		case '1', '2', '3', '4', '5':
			switch nat[:2] {
			case "10", "13", "15", "20", "23", "24", "26", "30", "35", "36", "38",
				"40", "43", "45", "46", "50", "53", "55", "58":
				set(2, 9)
			default:
				set(3, 9)
			}
		case '6', '7':
			set(2, 9)
		case '8', '9':
			set(3, 8, 10)
		default:
			err()
		}
	case "32": // Belgium (Jan20) en.wikipedia.org/wiki/Telephone_numbers_in_Belgium
		switch nat[0] {
		case '1', '5', '6', '7':
			set(2, 8)
		case '2', '3':
			set(1, 8)
		case '4':
			switch len(nat) {
			case 8:
				set(1, 8)
			default:
				// 32[493388184 7] no documentation supporting 10-digit mobile NSNs
				set(3, 9)
			}
		case '8':
			if nat[1:3] == "00" {
				set(3, 8)
			} else {
				set(2, 8)
			}
		case '9':
			if nat[1] == '0' {
				set(3, 8)
			} else {
				set(1, 8)
			}
		default:
			err()
		}
	case "33": // France (Jan20) en.wikipedia.org/wiki/Telephone_numbers_in_France
		switch nat[0] {
		case '0':
			err()
		default:
			set(1, 9)
		}
	case "34": // Spain (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Spain
		switch nat[0] {
		case '5':
			set(1, 9)
		case '6', '8', '9':
			set(3, 9)
		case '7':
			if nat[1] == '0' {
				set(2, 9)
			} else {
				set(3, 9)
			}
		default:
			err()
		}
	//case "350": // Gibraltar (3)
	//case "351": // Portugal (3)
	//case "352": // Luxembourg (3)
	case "353": // Ireland (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_the_Republic_of_Ireland
		switch nat[0] {
		case '1':
			set(1, 6, 8)
		case '2', '4', '5', '6', '7', '9':
			set(2, 7, 9)
		case '8':
			set(2, 9)
		default:
			err()
		}
	//case "354": // Iceland (3)
	//case "355": // Albania (3)
	//case "356": // Malta (3)
	//case "357": // Cyprus (3)
	//case "358": // Finland (3)
	//case "359": // Bulgaria (3)
	case "36": // Hungary (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Hungary
		switch nat[0] {
		case '0':
			err()
		case '1':
			set(1, 8)
		default:
			set(2, 8)
		}
	//case "370": // Lithuania (3)
	//case "371": // Latvia (3)
	//case "372": // Estonia (3)
	//case "373": // Moldova (3)
	//case "374": // Armenia (3)
	//case "375": // Belarus (3)
	//case "376": // Andorra (3)
	//case "377": // Monaco (3)
	//case "378": // San Marino (3)
	case "379": // Holy See (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Vatican_City
		err()
	//case "380": // Ukraine (3)
	//case "381": // Serbia (3)
	//case "382": // Montenegro (3)
	//case "385": // Croatia (3)
	//case "386": // Slovenia (2)
	//case "387": // Bosnia & Herzegovina (3)
	//case "388": // EU shared (0)
	//case "389": // Macedonia (3)
	case "39": // Italy (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Italy
		switch nat[0] {
		case '0':
			switch nat[1] {
			case '2', '6':
				set(2, 6, 11)
			default:
				set(3, 6, 11)
			}
		case '3':
			set(3, 9, 10)
		case '5':
			set(2, 10, 11)
		case '8':
			set(3, 6, 10)
		default:
			// '4', '7' implement?
			err()
		}

	case "40": // Romania (Jan21) https://en.wikipedia.org/wiki/Telephone_numbers_in_Romania
		switch nat[0] {
		case '2', '3':
			switch nat[1] {
			case '1':
				set(2, 9)
			case '3', '4', '5', '6', '7':
				set(3, 9)
			default:
				err()
			}
		case '6', '7', '8', '9':
			set(3, 9)
		default:
			err()
		}
	case "41": // Switzerland (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Switzerland
		switch nat[0] {
		case '2', '3', '4', '5', '6', '7':
			// 41[54576628] (old "54" area code, 1 digit short)
			// 41[67052669] ...
			set(2, 9)
		case '8', '9':
			switch nat[:2] {
			case "81", "91":
				set(2, 9)
			default:
				set(3, 9)
			}
		default:
			err()
		}
	//case "420": // Czech Republic (3)
	//case "421": // Slovakia (3)
	case "423": // Liechtenstein (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Liechtenstein
		switch nat[1] {
		case '2', '3', '7':
			set(1, 7)
		case '6':
			set(2, 9)
		case '8':
			set(2, 7)
		case '9':
			set(3, 7)
		default:
			err()
		}
	case "43": // Austria (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Austria
		switch nat[0] {
		case '1':
			set(1, 6, 8)
		case '2', '3', '4', '5', '7':
			switch nat[:3] {
			case "316", "463", "512", "732":
				set(3, 7, 9)
			default:
				set(4, 7, 9)
			}
		case '6':
			switch nat[1:3] {
			case "54", "56", "58", "60":
				set(4, 7, 9)
			case "62":
				set(3, 7, 9)
			default:
				if nat[1:3] < "50" {
					set(4, 7, 9)
				} else {
					set(3, 10, 12)
				}
			}
		case '8', '9':
			set(3, 8, 9)
		default:
			err()
		}
	case "44": // United Kingdom (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_the_United_Kingdom
		switch nat[0] {
		case '1':
			if nat[1] == '1' || nat[2] == '1' {
				set(3, 10)
			} else {
				set(4, 9, 10)
			}
		case '2', '5':
			set(2, 10)
		case '3', '8', '9':
			// 44[3452661037 2] answers with superfluous digit
			if set(3, 10) || nat[:3] == "800" && set(3, 9) {
			}
		case '7':
			switch nat[1] {
			case '0', '6':
				set(2, 10)
			default:
				set(4, 10)
			}
		default:
			err()
		}
	case "45": // Denmark (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Denmark
		switch nat[0] {
		case '0', '1':
			err()
		default:
			set(2, 8)
		}
	case "46": // Sweden (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Sweden
		switch nat[0] {
		case '0':
			err()
		case '7':
			set(2, 7, 9)
		case '8':
			set(1, 7, 9)
		default:
			switch nat[:2] {
			case "10", "11", "13", "16", "18", "19", "20", "21", "23", "26", "31", "33", "35", "36",
				"40", "42", "44", "46", "54", "60", "63", "90":
				set(2, 7, 9)
			default:
				set(3, 7, 9)
			}
		}
	case "47": // Norway (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Norway
		switch nat[0] {
		case '0':
			set(0, 5)
		case '2':
			set(1, 8)
		case '3', '4', '5', '6', '7', '9':
			set(2, 8)
		case '8':
			set(3, 8)
		default:
			err()
		}
	case "48": // Poland (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Poland
		switch nat[:2] {
		case "45", "50", "51", "53", "57", "60", "66", "69", "72", "73", "78", "79", "80", "88":
			set(3, 9)
		default:
			if nat[0] == '0' {
				err()
			} else {
				set(2, 9)
			}
		}
	case "49": // Germany (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Germany
		switch nat[:2] {
		case "15", "16", "17":
			set(3, 10, 11)
		case "30", "40", "69", "89":
			set(2, 7, 10)
		case "32":
			set(2, 11)
		default:
			switch nat[:3] {
			case "700", "800", "900":
				set(3, 10, 11)
			default:
				switch nat[0] {
				case '0', '1':
					err()
				default:
					// 49[228978915397] 1 digit too long
					set(4, 7, 11)
				}
			}
		}

	//case "500": // Falkland Islands (3)
	//case "501": // Belize (3)
	//case "502": // Guatemala (3)
	//case "503": // El Salvador (3)
	//case "504": // Honduras (3)
	//case "505": // Nicaragua (3)
	//case "506": // Costa Rica (3)
	//case "507": // Panama (3)
	//case "508": // Saint Pierre & Miquelon (3)
	//case "509": // Haiti (3)
	case "51": // Peru (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Peru
		switch nat[0] {
		case '1':
			set(1, 8)
		case '4', '5', '6', '7', '8':
			set(2, 8)
		case '9':
			// 51[92225419]
			set(3, 9)
		default:
			err()
		}
	case "52": // Mexico (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Mexico
		switch nat[0] {
		case '2', '3', '4', '5', '6', '7', '8', '9':
			switch nat[:2] {
			case "33", "55", "56", "81":
				set(2, 10)
			default:
				set(3, 10)
			}
		default:
			err()
		}
	case "53": // Cuba (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Cuba
		switch nat[0] {
		case '2', '3', '4':
			set(2, 6, 8)
		case '5':
			set(1, 8)
		case '7':
			set(1, 6, 8)
		default:
			err()
		}
	case "54": // Argentina (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Argentina
		switch nat[0] {
		case '0':
			err()
		case '1':
			set(2, 10)
		case '9':
			set(4, 11)
		default:
			set(3, 10)
		}
	case "55": // Brazil (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Brazil en.wikipedia.org/wiki/List_of_dialling_codes_in_Brazil
		switch nat[0] {
		case '0':
			err()
		default:
			switch nat[1] {
			case '0':
				set(3, 8, 10)
			default:
				switch nat[2] {
				case '2', '3', '4', '5':
					set(2, 10)
				case '6', '7', '8', '9':
					set(3, 11)
				default:
					err()
				}
			}
		}
	case "56": // Chile (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Chile
		switch nat[0] {
		case '0':
			err()
		default:
			set(0, 9)
		}
	case "57": // Colombia (Jan20) en.wikipedia.org/wiki/Telephone_numbers_in_Colombia
		switch nat[0] {
		case '1', '2', '4', '5', '6', '7', '8':
			set(1, 8)
		case '3':
			set(3, 10)
		default:
			err()
		}
	case "58": // Venezuela (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Venezuela
		switch nat[0] {
		case '2', '4', '5', '8', '9':
			set(3, 10)
		default:
			err()
		}
	//case "590": // Guadeloupe (3)
	//case "591": // Bolivia (3)
	//case "592": // Guyana (3)
	//case "593": // Ecuador (3)
	//case "594": // French Guiana (3)
	//case "595": // Paraguay (3)
	//case "596": // Martinique (3)
	//case "597": // Suriname (3)
	//case "598": // Uruguay (3)
	//case "599": // Caribbean Netherlands (3)

	case "60": // Malaysia (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Malaysia
		switch nat[0] {
		case '1':
			switch nat[:4] {
			case "1300", "1500", "1508", "1600", "1700", "1800", "1900":
				set(4, 10)
			default:
				switch nat[1] {
				case '1', '5':
					set(2, 10)
				default:
					set(2, 9)
				}
			}
		case '3':
			set(1, 9)
		case '4', '5', '6', '7', '9':
			set(1, 8)
		case '8':
			set(2, 8)
		default:
			err()
		}
	case "61": // Australia (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Australia
		switch nat[0] {
		case '1', '2', '3', '7':
			switch nat[:2] {
			case "14", "27", "28", "29", "37", "38", "39", "72", "73":
				set(2, 9)
			default:
				set(3, 9)
			}
		case '4', '5', '8':
			set(3, 9)
		default:
			err()
		}
	case "62": // Indonesia (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Indonesia
		switch nat[0] {
		case '0', '1':
			err()
		case '8':
			set(3, 10, 13)
		default:
			switch nat[:2] {
			case "21", "31":
				set(2, 7, 11)
			default:
				set(3, 7, 11)
			}
		}
	case "63": // Philippines (Jan20) en.wikipedia.org/wiki/Telephone_numbers_in_the_Philippines
		switch nat[0] {
		case '2':
			set(1, 9)
		case '3', '4', '5', '6', '7', '8':
			set(2, 9)
		case '9':
			set(3, 10)
		default:
			err()
		}
	case "64": // New Zealand (Jan20) en.wikipedia.org/wiki/Telephone_numbers_in_New_Zealand
		switch nat[0] {
		case '3', '4', '6', '7', '9':
			set(3, 8)
		case '2':
			set(3, 8, 10)
		default:
			err()
		}
	case "65": // Singapore (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Singapore
		switch nat[0] {
		case '1':
			switch nat[1:4] {
			case "800", "900":
				set(4, 11)
			default:
				err()
			}
		case '3', '6', '9':
			set(1, 8)
		case '8':
			switch nat[1:3] {
			case "00":
				set(3, 10)
			default:
				set(1, 8)
			}
		default:
			err()
		}
	case "66": // Thailand (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Thailand
		switch nat[0] {
		case '1':
			switch nat[1:4] {
			case "401", "800", "900":
				set(4, 10)
			default:
				err()
			}
		case '2':
			set(1, 7, 8)
		case '3', '4', '5', '7':
			set(2, 8)
		case '6', '8', '9':
			set(2, 9)
		default:
			err()
		}
	//case "670": // Timor-Leste (3)
	//case "672": // Australian External Territories (3)
	//case "673": // Brunei Darussalam (3)
	//case "674": // Nauru (3)
	//case "675": // Papua New Guinea (3)
	//case "676": // Tonga (3)
	//case "677": // Solomon Islands (3)
	//case "678": // Vanuatu (3)
	//case "679": // Fiji (3)
	//case "680": // Palau (3)
	//case "681": // Wallis & Futuna (3)
	//case "682": // Cook Islands (3)
	//case "683": // Niue (3)
	//case "685": // Samoa (3)
	//case "686": // Kiribati (3)
	//case "687": // New Caledonia (3)
	//case "688": // Tuvalu (3)
	//case "689": // French Polynesia (3)
	case "690": // Tokelau (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Tokelau
		switch nat[0] {
		case '0':
			err()
		case '2':
			set(1, 5)
		default:
			set(0, 5)
		}
	//case "691": // Micronesia (3)
	//case "692": // Marshall Islands (3)

	case "7": // Russia & Kazakhstan (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Russia en.wikipedia.org/wiki/Telephone_numbers_in_Kazakhstan
		switch nat[0] {
		case '3', '4', '6', '7', '8', '9':
			set(3, 10)
		default:
			err()
		}

	case "800": // global freephone (Jan21) en.wikipedia.org/wiki/Toll-free_telephone_number#International
		set(0, 8)
	case "808": // global shared cost (Jan21) en.wikipedia.org/wiki/Shared-cost_service itu.int/en/ITU-T/inr/unum/Pages/uiscn.aspx
		set(0, 8)
	case "81": // Japan (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Japan
		if nat[0] == '0' {
			err()
		} else if nat[1] == '0' {
			set(2, 9, 10)
		} else if nat[2] == '0' {
			switch nat[:2] {
			case "12", "13", "14", "16", "17", "18", "19", "57", "80", "91", "99":
				set(3, 9)
			default:
				set(2, 9)
			}
		} else {
			set(2, 9)
		}
	case "82": // Korea (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_South_Korea
		switch nat[0] {
		case '1':
			set(2, 8, 10)
		case '2':
			set(1, 8, 9)
		case '3', '4', '5', '6', '7', '8':
			set(2, 9, 10)
		default:
			err()
		}
	case "84": // Vietnam (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Vietnam
		switch nat[0] {
		case '1':
			if nat[1:4] == "900" {
				set(4, 8, 10)
			} else {
				err()
			}
		case '2':
			set(3, 10)
		case '3', '5', '6', '7', '8', '9':
			set(2, 9)
		default:
			err()
		}
	//case "850": // Korea DPR (3)
	//case "852": // Hong Kong (1)
	//case "853": // Macao (3)
	//case "855": // Cambodia (3)
	//case "856": // Laos (3)
	case "86": // China (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_China
		switch nat[0] {
		case '0':
			err()
		case '1':
			if nat[1] == 0 {
				set(2, 10)
			} else {
				set(3, 11)
			}
		case '2':
			set(2, 10)
		default:
			set(3, 10, 11)
		}
	case "870": // global Inmarsat (Jan21) en.wikipedia.org/wiki/Inmarsat
		set(0, 4, 12)
	case "878": // global personal numbers (Jan21) en.wikipedia.org/wiki/Universal_Personal_Telecommunications
		switch nat[:2] {
		case "10":
			set(2, 12)
		default:
			err()
		}
	//case "880": // Bangladesh (3)
	case "881": // global satphone (Jan21) en.wikipedia.org/wiki/Global_Mobile_Satellite_System
		set(1, 5, 12)
	case "882": // global 882 (Jan21) en.wikipedia.org/wiki/International_Networks_(country_code)
		switch nat[0] {
		case '1', '2', '3', '4', '8', '9':
			set(2, 6, 12)
		default:
			err()
		}
	case "883": // global 883 (Jan21) en.wikipedia.org/wiki/International_Networks_(country_code)
		switch nat[0] {
		case '0':
			set(2, 6, 12)
		case '1', '2':
			set(3, 7, 12)
		case '5':
			set(4, 8, 12)
		default:
			err()
		}
	//case "886": // Taiwan (3)
	case "888": // global humanitarian affairs (Jan21) en.wikipedia.org/wiki/United_Nations_Office_for_the_Coordination_of_Humanitarian_Affairs
		set(0, 4, 12)

	case "90": // Turkey (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Turkey
		switch nat[0] {
		case '2', '3', '4', '5':
			set(3, 10)
		default:
			switch nat[:3] {
			case "800", "822", "850", "900":
				set(3, 10)
			default:
				err()
			}
		}
	case "91": // India (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_India
		switch nat[:2] {
		case "11", "20", "22", "33", "40", "44", "79", "80":
			set(2, 10)
		default:
			switch nat[0] {
			case '0':
				err()
			case '6', '7', '8', '9':
				set(4, 10)
			default:
				set(3, 10)
			}
		}
	case "92": // Pakistan (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Pakistan
		switch nat[0] {
		case '2', '4', '5', '6', '7':
			set(2, 9, 10)
		case '3':
			set(2, 10)
		case '8', '9':
			switch nat[1:3] {
			case "00":
				set(3, 8)
			default:
				set(2, 9, 10)
			}
		default:
		}
	case "93": // Afghanistan (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Afghanistan
		switch nat[0] {
		case '2', '3', '4', '5', '6', '7':
			set(2, 9)
		default:
			err()
		}
	case "94": // Sri Lanka (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Sri_Lanka
		switch nat[0] {
		case '0':
			err()
		case '7':
			set(2, 9)
		default:
			set(3, 9)
		}
	case "95": // Myanmar (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Myanmar
		switch nat[0] {
		case '0':
			err()
		case '9':
			set(2, 7, 10)
		default:
			set(4, 7, 10)
		}
	//case "960": // Maldives (3)
	//case "961": // Lebanon (3)
	//case "962": // Jordan (3)
	//case "963": // Syria (3)
	//case "964": // Iraq (3)
	//case "965": // Kuwait (4)
	//case "966": // Saudi Arabia (3)
	//case "967": // Yemen (3)
	//case "968": // Oman (3)
	//case "970": // Palestine (3)
	//case "971": // United Arab Emirates (3)
	//case "972": // Israel (1)
	//case "973": // Bahrain (3)
	//case "974": // Qatar (3)
	//case "975": // Bhutan (3)
	//case "976": // Mongolia (3)
	//case "977": // Nepal (3)
	case "979": // global premium rate (Jan21) en.wikipedia.org/wiki/International_Premium_Rate_Service
		switch nat[0] {
		case '1', '3', '5', '9':
			set(1, 9)
		default:
			err()
		}
	case "98": // Iran (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Iran
		switch nat[0] {
		case '0':
			err()
		case '8':
			switch nat[1:4] {
			case "080", "081":
				set(4, 8)
			default:
				set(2, 10)
			}
		case '9':
			set(3, 10)
		default:
			set(2, 10)
		}
	case "991": // global ITPCS trial (Jan21; defunct?) en.wikipedia.org/wiki/ITPCS
		switch nat[:3] {
		case "001":
			set(3, 7, 12)
		default:
			err()
		}
		//case "992": // Tajikistan (3)
		//case "993": // Turkmenistan (3)
		//case "994": // Azerbaijan (3)
		//case "995": // Georgia (3)
		//case "996": // Kyrgyzstan (3)
		//case "998": // Uzbekistan (3)
	}
	return
}
