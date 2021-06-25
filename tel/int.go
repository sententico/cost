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
		"8":	{"Name":"Inteliquent",	"Alias":["INTELIQUENT","inteliquent","IQ","iq","NT","nt","INTLQNT","INTLQTSD"]},
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

		"6":	{"Name":"DUB",			"Alias":["#UK standby", "dub",		"UKSSPRD...RBBN"]},
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
		"211":	{"Geo":"afr",	"ISO3166":"SS",	"Pl":2,	"CCn":"South Sudan"},
		"212":	{"Geo":"afr",	"ISO3166":"MA",	"Pl":3,	"CCn":"Morocco"},
		"213":	{"Geo":"afr",	"ISO3166":"DZ",	"Pl":2,	"CCn":"Algeria"},
		"216":	{"Geo":"afr",	"ISO3166":"TN",	"Pl":2,	"CCn":"Tunisia"},
		"218":	{"Geo":"afr",	"ISO3166":"LY",	"Pl":2,	"CCn":"Libya"},
		"220":	{"Geo":"afr",	"ISO3166":"GM",	"Pl":1,	"CCn":"Gambia"},
		"221":	{"Geo":"afr",	"ISO3166":"SN",	"Pl":2,	"CCn":"Senegal"},
		"222":	{"Geo":"afr",	"ISO3166":"MR",	"Pl":1,	"CCn":"Mauritania"},
		"223":	{"Geo":"afr",	"ISO3166":"ML",	"Pl":2,	"CCn":"Mali"},
		"224":	{"Geo":"afr",	"ISO3166":"GN",	"Pl":2,	"CCn":"Guinea"},
		"225":	{"Geo":"afr",	"ISO3166":"CI",	"Pl":2,	"CCn":"Ivory Coast"},
		"226":	{"Geo":"afr",	"ISO3166":"BF",	"Pl":2,	"CCn":"Burkina Faso"},
		"227":	{"Geo":"afr",	"ISO3166":"NE",	"Pl":2,	"CCn":"Niger"},
		"228":	{"Geo":"afr",	"ISO3166":"TG",	"Pl":2,	"CCn":"Togo"},
		"229":	{"Geo":"afr",	"ISO3166":"BJ",	"Pl":2,	"CCn":"Benin"},
		"230":	{"Geo":"afr",	"ISO3166":"MU",	"Pl":2,	"CCn":"Mauritius"},
		"231":	{"Geo":"afr",	"ISO3166":"LR",	"Pl":2,	"CCn":"Liberia"},
		"232":	{"Geo":"afr",	"ISO3166":"SL",	"Pl":2,	"CCn":"Sierra Leone"},
		"233":	{"Geo":"afr",	"ISO3166":"GH",	"Pl":2,	"CCn":"Ghana"},
		"234":	{"Geo":"afr",	"ISO3166":"NG",	"Pl":3,	"CCn":"Nigeria"},
		"235":	{"Geo":"afr",	"ISO3166":"TD",	"Pl":2,	"CCn":"Chad"},
		"236":	{"Geo":"afr",	"ISO3166":"CF",	"Pl":2,	"CCn":"Central African Republic"},
		"237":	{"Geo":"afr",	"ISO3166":"CM",	"Pl":2,	"CCn":"Cameroon"},
		"238":	{"Geo":"afr",	"ISO3166":"CV",	"Pl":2,	"CCn":"Cape Verde"},
		"239":	{"Geo":"afr",	"ISO3166":"ST",	"Pl":2,	"CCn":"Sao Tome & Principe"},
		"240":	{"Geo":"afr",	"ISO3166":"GQ",	"Pl":2,	"CCn":"Equatorial Guinea"},
		"241":	{"Geo":"afr",	"ISO3166":"GA",	"Pl":2,	"CCn":"Gabon"},
		"242":	{"Geo":"afr",	"ISO3166":"CG",	"Pl":2,	"CCn":"Congo"},
		"243":	{"Geo":"afr",	"ISO3166":"CD",	"Pl":2,	"CCn":"Congo DR"},
		"244":	{"Geo":"afr",	"ISO3166":"AO",	"Pl":2,	"CCn":"Angola"},
		"245":	{"Geo":"afr",	"ISO3166":"GW",	"Pl":2,	"CCn":"Guinea-Bissau"},
		"246":	{"Geo":"afr",	"ISO3166":"IO",	"Pl":3,	"CCn":"Diego Garcia"},
		"247":	{"Geo":"afr",	"ISO3166":"SH",	"Pl":1,	"CCn":"Ascension"},
		"248":	{"Geo":"afr",	"ISO3166":"SC",	"Pl":2,	"CCn":"Seychelles"},
		"249":	{"Geo":"afr",	"ISO3166":"SD",	"Pl":3,	"CCn":"Sudan"},
		"250":	{"Geo":"afr",	"ISO3166":"RW",	"Pl":2,	"CCn":"Rwanda"},
		"251":	{"Geo":"afr",	"ISO3166":"ET",	"Pl":2,	"CCn":"Ethiopia"},
		"252":	{"Geo":"afr",	"ISO3166":"SO",	"Pl":2,	"CCn":"Somalia"},
		"253":	{"Geo":"afr",	"ISO3166":"DJ",	"Pl":2,	"CCn":"Djibouti"},
		"254":	{"Geo":"afr",	"ISO3166":"KE",	"Pl":2,	"CCn":"Kenya"},
		"255":	{"Geo":"afr",	"ISO3166":"TZ",	"Pl":2,	"CCn":"Tanzania"},
		"256":	{"Geo":"afr",	"ISO3166":"UG",	"Pl":2,	"CCn":"Uganda"},
		"257":	{"Geo":"afr",	"ISO3166":"BI",	"Pl":2,	"CCn":"Burundi"},
		"258":	{"Geo":"afr",	"ISO3166":"MZ",	"Pl":2,	"CCn":"Mozambique"},
		"260":	{"Geo":"afr",	"ISO3166":"ZM",	"Pl":2,	"CCn":"Zambia"},
		"261":	{"Geo":"afr",	"ISO3166":"MG",	"Pl":2,	"CCn":"Madagascar"},
		"262":	{"Geo":"afr",	"ISO3166":"RE",	"Pl":3,	"CCn":"Reunion"},
		"263":	{"Geo":"afr",	"ISO3166":"ZW",	"Pl":2,	"CCn":"Zimbabwe"},
		"264":	{"Geo":"afr",	"ISO3166":"NA",	"Pl":2,	"CCn":"Namibia"},
		"265":	{"Geo":"afr",	"ISO3166":"MW",	"Pl":2,	"CCn":"Malawi"},
		"266":	{"Geo":"afr",	"ISO3166":"LS",	"Pl":2,	"CCn":"Lesotho"},
		"267":	{"Geo":"afr",	"ISO3166":"BW",	"Pl":2,	"CCn":"Botswana"},
		"268":	{"Geo":"afr",	"ISO3166":"SZ",	"Pl":2,	"CCn":"Eswatini"},
		"269":	{"Geo":"afr",	"ISO3166":"KM",	"Pl":2,	"CCn":"Comoros"},
		"27":	{"Geo":"afr",	"ISO3166":"ZA",	"Pl":2,	"CCn":"South Africa"},
		"290":	{"Geo":"afr",	"ISO3166":"SH",	"Pl":1,	"CCn":"Saint Helena & Tristan da Cunha"},
		"291":	{"Geo":"afr",	"ISO3166":"ER",	"Pl":1,	"CCn":"Eritrea"},
		"297":	{"Geo":"lam",	"ISO3166":"AW",	"Pl":3,	"CCn":"Aruba"},
		"298":	{"Geo":"eur",	"ISO3166":"FO",	"Pl":2,	"CCn":"Faroe Islands"},
		"299":	{"Geo":"eur",	"ISO3166":"GL",	"Pl":2,	"CCn":"Greenland"},

		"30":	{"Geo":"eur",	"ISO3166":"GR",	"Pl":3,	"CCn":"Greece"},
		"31":	{"Geo":"eur",	"ISO3166":"NL",	"Pl":2,	"CCn":"Netherlands"},
		"32":	{"Geo":"eur",	"ISO3166":"BE",	"Pl":2,	"CCn":"Belgium"},
		"33":	{"Geo":"eur",	"ISO3166":"FR",	"Pl":1,	"CCn":"France"},
		"34":	{"Geo":"eur",	"ISO3166":"ES",	"Pl":3,	"CCn":"Spain"},
		"350":	{"Geo":"eur",	"ISO3166":"GI",	"Pl":2,	"CCn":"Gibraltar"},
		"351":	{"Geo":"eur",	"ISO3166":"PT",	"Pl":2,	"CCn":"Portugal"},
		"352":	{"Geo":"eur",	"ISO3166":"LU",	"Pl":3,	"CCn":"Luxembourg"},
		"353":	{"Geo":"eur",	"ISO3166":"IE",	"Pl":2,	"CCn":"Ireland"},
		"354":	{"Geo":"eur",	"ISO3166":"IS",	"Pl":3,	"CCn":"Iceland"},
		"355":	{"Geo":"eur",	"ISO3166":"AL",	"Pl":2,	"CCn":"Albania"},
		"356":	{"Geo":"eur",	"ISO3166":"MT",	"Pl":2,	"CCn":"Malta"},
		"357":	{"Geo":"eur",	"ISO3166":"CY",	"Pl":2,	"CCn":"Cyprus"},
		"358":	{"Geo":"eur",	"ISO3166":"FI",	"Pl":2,	"CCn":"Finland"},
		"359":	{"Geo":"eur",	"ISO3166":"BG",	"Pl":3,	"CCn":"Bulgaria"},
		"36":	{"Geo":"eur",	"ISO3166":"HU",	"Pl":2,	"CCn":"Hungary"},
		"370":	{"Geo":"eur",	"ISO3166":"LT",	"Pl":3,	"CCn":"Lithuania"},
		"371":	{"Geo":"eur",	"ISO3166":"LV",	"Pl":3,	"CCn":"Latvia"},
		"372":	{"Geo":"eur",	"ISO3166":"EE",	"Pl":2,	"CCn":"Estonia"},
		"373":	{"Geo":"eur",	"ISO3166":"MD",	"Pl":2,	"CCn":"Moldova"},
		"374":	{"Geo":"eur",	"ISO3166":"AM",	"Pl":2,	"CCn":"Armenia"},
		"375":	{"Geo":"eur",	"ISO3166":"BY",	"Pl":2,	"CCn":"Belarus"},
		"376":	{"Geo":"eur",	"ISO3166":"AD",	"Pl":1,	"CCn":"Andorra"},
		"377":	{"Geo":"eur",	"ISO3166":"MC",	"Pl":2,	"CCn":"Monaco"},
		"378":	{"Geo":"eur",	"ISO3166":"SM",	"Pl":2,	"CCn":"San Marino"},
		"379":	{"Geo":"eur",	"ISO3166":"VA",	"Pl":0,	"CCn":"Holy See"},
		"380":	{"Geo":"eur",	"ISO3166":"UA",	"Pl":2,	"CCn":"Ukraine"},
		"381":	{"Geo":"eur",	"ISO3166":"RS",	"Pl":2,	"CCn":"Serbia"},
		"382":	{"Geo":"eur",	"ISO3166":"ME",	"Pl":2,	"CCn":"Montenegro"},
		"383":	{"Geo":"eur",	"ISO3166":"XK",	"Pl":2,	"CCn":"Kosovo"},
		"385":	{"Geo":"eur",	"ISO3166":"HR",	"Pl":2,	"CCn":"Croatia"},
		"386":	{"Geo":"eur",	"ISO3166":"SI",	"Pl":2,	"CCn":"Slovenia"},
		"387":	{"Geo":"eur",	"ISO3166":"BA",	"Pl":2,	"CCn":"Bosnia & Herzegovina"},
		"389":	{"Geo":"eur",	"ISO3166":"MK",	"Pl":3,	"CCn":"Macedonia"},
		"39":	{"Geo":"eur",	"ISO3166":"IT",	"Pl":3,	"CCn":"Italy"},

		"40":	{"Geo":"eur",	"ISO3166":"RO",	"Pl":3,	"CCn":"Romania"},
		"41":	{"Geo":"eur",	"ISO3166":"CH",	"Pl":2,	"CCn":"Switzerland"},
		"420":	{"Geo":"eur",	"ISO3166":"CZ",	"Pl":3,	"CCn":"Czech Republic"},
		"421":	{"Geo":"eur",	"ISO3166":"SK",	"Pl":2,	"CCn":"Slovakia"},
		"423":	{"Geo":"eur",	"ISO3166":"LI",	"Pl":1,	"CCn":"Liechtenstein"},
		"43":	{"Geo":"eur",	"ISO3166":"AT",	"Pl":3,	"CCn":"Austria"},
		"44":	{"Geo":"eur",	"ISO3166":"GB",	"Pl":3,	"CCn":"United Kingdom"},
		"45":	{"Geo":"eur",	"ISO3166":"DK",	"Pl":2,	"CCn":"Denmark"},
		"46":	{"Geo":"eur",	"ISO3166":"SE",	"Pl":2,	"CCn":"Sweden"},
		"47":	{"Geo":"eur",	"ISO3166":"NO",	"Pl":2,	"CCn":"Norway"},
		"48":	{"Geo":"eur",	"ISO3166":"PL",	"Pl":2,	"CCn":"Poland"},
		"49":	{"Geo":"eur",	"ISO3166":"DE",	"Pl":3,	"CCn":"Germany"},

		"500":	{"Geo":"lam",	"ISO3166":"FK",	"Pl":1,	"CCn":"Falkland Islands"},
		"501":	{"Geo":"lam",	"ISO3166":"BZ",	"Pl":2,	"CCn":"Belize"},
		"502":	{"Geo":"lam",	"ISO3166":"GT",	"Pl":1,	"CCn":"Guatemala"},
		"503":	{"Geo":"lam",	"ISO3166":"SV",	"Pl":2,	"CCn":"El Salvador"},
		"504":	{"Geo":"lam",	"ISO3166":"HN",	"Pl":2,	"CCn":"Honduras"},
		"505":	{"Geo":"lam",	"ISO3166":"NI",	"Pl":2,	"CCn":"Nicaragua"},
		"506":	{"Geo":"lam",	"ISO3166":"CR",	"Pl":2,	"CCn":"Costa Rica"},
		"507":	{"Geo":"lam",	"ISO3166":"PA",	"Pl":2,	"CCn":"Panama"},
		"508":	{"Geo":"lam",	"ISO3166":"PM",	"Pl":0,	"CCn":"Saint Pierre & Miquelon"},
		"509":	{"Geo":"lam",	"ISO3166":"HT",	"Pl":2,	"CCn":"Haiti"},
		"51":	{"Geo":"lam",	"ISO3166":"PE",	"Pl":3,	"CCn":"Peru"},
		"52":	{"Geo":"lam",	"ISO3166":"MX",	"Pl":3,	"CCn":"Mexico"},
		"53":	{"Geo":"lam",	"ISO3166":"CU",	"Pl":1,	"CCn":"Cuba"},
		"54":	{"Geo":"lam",	"ISO3166":"AR",	"Pl":3,	"CCn":"Argentina"},
		"55":	{"Geo":"lam",	"ISO3166":"BR",	"Pl":2,	"CCn":"Brazil"},
		"56":	{"Geo":"lam",	"ISO3166":"CL",	"Pl":0,	"CCn":"Chile"},
		"57":	{"Geo":"lam",	"ISO3166":"CO",	"Pl":1,	"CCn":"Colombia"},
		"58":	{"Geo":"lam",	"ISO3166":"VE",	"Pl":3,	"CCn":"Venezuela"},
		"590":	{"Geo":"lam",	"ISO3166":"GP",	"Pl":3,	"CCn":"Guadeloupe"},
		"591":	{"Geo":"lam",	"ISO3166":"BO",	"Pl":1,	"CCn":"Bolivia"},
		"592":	{"Geo":"lam",	"ISO3166":"GY",	"Pl":3,	"CCn":"Guyana"},
		"593":	{"Geo":"lam",	"ISO3166":"EC",	"Pl":2,	"CCn":"Ecuador"},
		"594":	{"Geo":"lam",	"ISO3166":"GF",	"Pl":3,	"CCn":"French Guiana"},
		"595":	{"Geo":"lam",	"ISO3166":"PY",	"Pl":3,	"CCn":"Paraguay"},
		"596":	{"Geo":"lam",	"ISO3166":"MQ",	"Pl":3,	"CCn":"Martinique"},
		"597":	{"Geo":"lam",	"ISO3166":"SR",	"Pl":2,	"CCn":"Suriname"},
		"598":	{"Geo":"lam",	"ISO3166":"UY",	"Pl":2,	"CCn":"Uruguay"},
		"599":	{"Geo":"lam",	"ISO3166":"CW",	"Pl":3,	"CCn":"Caribbean Netherlands"},

		"60":	{"Geo":"apac",	"ISO3166":"MY",	"Pl":2,	"CCn":"Malaysia"},
		"61":	{"Geo":"apac",	"ISO3166":"AU",	"Pl":3,	"CCn":"Australia"},
		"62":	{"Geo":"apac",	"ISO3166":"ID",	"Pl":3,	"CCn":"Indonesia"},
		"63":	{"Geo":"apac",	"ISO3166":"PH",	"Pl":2,	"CCn":"Philippines"},
		"64":	{"Geo":"apac",	"ISO3166":"NZ",	"Pl":3,	"CCn":"New Zealand"},
		"65":	{"Geo":"apac",	"ISO3166":"SG",	"Pl":1,	"CCn":"Singapore"},
		"66":	{"Geo":"apac",	"ISO3166":"TH",	"Pl":2,	"CCn":"Thailand"},
		"670":	{"Geo":"apac",	"ISO3166":"TL",	"Pl":2,	"CCn":"Timor-Leste"},
		"672":	{"Geo":"apac",	"ISO3166":"NF",	"Pl":2,	"CCn":"Norfolk Island"},
		"673":	{"Geo":"apac",	"ISO3166":"BN",	"Pl":3,	"CCn":"Brunei Darussalam"},
		"674":	{"Geo":"apac",	"ISO3166":"NR",	"Pl":3,	"CCn":"Nauru"},
		"675":	{"Geo":"apac",	"ISO3166":"PG",	"Pl":2,	"CCn":"Papua New Guinea"},
		"676":	{"Geo":"apac",	"ISO3166":"TO",	"Pl":3,	"CCn":"Tonga"},
		"677":	{"Geo":"apac",	"ISO3166":"SB",	"Pl":2,	"CCn":"Solomon Islands"},
		"678":	{"Geo":"apac",	"ISO3166":"VU",	"Pl":2,	"CCn":"Vanuatu"},
		"679":	{"Geo":"apac",	"ISO3166":"FJ",	"Pl":2,	"CCn":"Fiji"},
		"680":	{"Geo":"apac",	"ISO3166":"PW",	"Pl":3,	"CCn":"Palau"},
		"681":	{"Geo":"apac",	"ISO3166":"WF",	"Pl":2,	"CCn":"Wallis & Futuna"},
		"682":	{"Geo":"apac",	"ISO3166":"CK",	"Pl":2,	"CCn":"Cook Islands"},
		"683":	{"Geo":"apac",	"ISO3166":"NU",	"Pl":3,	"CCn":"Niue"},
		"685":	{"Geo":"apac",	"ISO3166":"WS",	"Pl":2,	"CCn":"Samoa"},
		"686":	{"Geo":"apac",	"ISO3166":"KI",	"Pl":3,	"CCn":"Kiribati"},
		"687":	{"Geo":"apac",	"ISO3166":"NC",	"Pl":2,	"CCn":"New Caledonia"},
		"688":	{"Geo":"apac",	"ISO3166":"TV",	"Pl":2,	"CCn":"Tuvalu"},
		"689":	{"Geo":"apac",	"ISO3166":"PF",	"Pl":2,	"CCn":"French Polynesia"},
		"690":	{"Geo":"apac",	"ISO3166":"TK",	"Pl":0,	"CCn":"Tokelau"},
		"691":	{"Geo":"apac",	"ISO3166":"FM",	"Pl":3,	"CCn":"Micronesia"},
		"692":	{"Geo":"apac",	"ISO3166":"MH",	"Pl":3,	"CCn":"Marshall Islands"},

		"7":	{"Geo":"rus",	"ISO3166":"XC",	"Pl":1,	"CCn":"Russia & Kazakhstan", "Sub":[
				{"Geo":"mea",	"ISO3166":"KZ",	"Pl":3, "CCn":"Kazakhstan",
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
		"853":	{"Geo":"apac",	"ISO3166":"MO",	"Pl":2,	"CCn":"Macao"},
		"855":	{"Geo":"apac",	"ISO3166":"KH",	"Pl":2,	"CCn":"Cambodia"},
		"856":	{"Geo":"apac",	"ISO3166":"LA",	"Pl":2,	"CCn":"Laos"},
		"86":	{"Geo":"apac",	"ISO3166":"CN",	"Pl":3,	"CCn":"China"},
		"870":	{"Geo":"glob",	"ISO3166":"XC",	"Pl":0,	"CCn":"global Inmarsat"},
		"878":	{"Geo":"glob",	"ISO3166":"XC",	"Pl":2,	"CCn":"global personal numbers"},
		"880":	{"Geo":"apac",	"ISO3166":"BD",	"Pl":3,	"CCn":"Bangladesh"},
		"881":	{"Geo":"glob",	"ISO3166":"XC",	"Pl":1,	"CCn":"global satphone"},
		"882":	{"Geo":"glob",	"ISO3166":"XC",	"Pl":2,	"CCn":"global 882"},
		"883":	{"Geo":"glob",	"ISO3166":"XC",	"Pl":3,	"CCn":"global 883"},
		"886":	{"Geo":"apac",	"ISO3166":"TW",	"Pl":2,	"CCn":"Taiwan"},
		"888":	{"Geo":"glob",	"ISO3166":"XC",	"Pl":0,	"CCn":"global humanitarian affairs"},

		"90":	{"Geo":"mea",	"ISO3166":"TR",	"Pl":3,	"CCn":"Turkey"},
		"91":	{"Geo":"mea",	"ISO3166":"IN",	"Pl":4,	"CCn":"India"},
		"92":	{"Geo":"mea",	"ISO3166":"PK",	"Pl":2,	"CCn":"Pakistan"},
		"93":	{"Geo":"mea",	"ISO3166":"AF",	"Pl":2,	"CCn":"Afghanistan"},
		"94":	{"Geo":"mea",	"ISO3166":"LK",	"Pl":3,	"CCn":"Sri Lanka"},
		"95":	{"Geo":"mea",	"ISO3166":"MM",	"Pl":4,	"CCn":"Myanmar"},
		"960":	{"Geo":"mea",	"ISO3166":"MV",	"Pl":2,	"CCn":"Maldives"},
		"961":	{"Geo":"mea",	"ISO3166":"LB",	"Pl":2,	"CCn":"Lebanon"},
		"962":	{"Geo":"mea",	"ISO3166":"JO",	"Pl":2,	"CCn":"Jordan"},
		"963":	{"Geo":"mea",	"ISO3166":"SY",	"Pl":2,	"CCn":"Syria"},
		"964":	{"Geo":"mea",	"ISO3166":"IQ",	"Pl":2,	"CCn":"Iraq"},
		"965":	{"Geo":"mea",	"ISO3166":"KW",	"Pl":2,	"CCn":"Kuwait"},
		"966":	{"Geo":"mea",	"ISO3166":"SA",	"Pl":2,	"CCn":"Saudi Arabia"},
		"967":	{"Geo":"mea",	"ISO3166":"YE",	"Pl":2,	"CCn":"Yemen"},
		"968":	{"Geo":"mea",	"ISO3166":"OM",	"Pl":2,	"CCn":"Oman"},
		"970":	{"Geo":"mea",	"ISO3166":"PS",	"Pl":2,	"CCn":"Palestine"},
		"971":	{"Geo":"mea",	"ISO3166":"AE",	"Pl":2,	"CCn":"United Arab Emirates"},
		"972":	{"Geo":"mea",	"ISO3166":"IL",	"Pl":2,	"CCn":"Israel"},
		"973":	{"Geo":"mea",	"ISO3166":"BH",	"Pl":2,	"CCn":"Bahrain"},
		"974":	{"Geo":"mea",	"ISO3166":"QA",	"Pl":2,	"CCn":"Qatar"},
		"975":	{"Geo":"mea",	"ISO3166":"BT",	"Pl":2,	"CCn":"Bhutan"},
		"976":	{"Geo":"mea",	"ISO3166":"MN",	"Pl":2,	"CCn":"Mongolia"},
		"977":	{"Geo":"mea",	"ISO3166":"NP",	"Pl":2,	"CCn":"Nepal"},
		"979":	{"Geo":"glob",	"ISO3166":"XC",	"Pl":1,	"CCn":"global premium rate"},
		"98":	{"Geo":"mea",	"ISO3166":"IR",	"Pl":3,	"CCn":"Iran"},
		"991":	{"Geo":"glob",	"ISO3166":"XC",	"Pl":3,	"CCn":"global ITPCS trial"},
		"992":	{"Geo":"mea",	"ISO3166":"TJ",	"Pl":2,	"CCn":"Tajikistan"},
		"993":	{"Geo":"mea",	"ISO3166":"TM",	"Pl":2,	"CCn":"Turkmenistan"},
		"994":	{"Geo":"mea",	"ISO3166":"AZ",	"Pl":2,	"CCn":"Azerbaijan"},
		"995":	{"Geo":"mea",	"ISO3166":"GE",	"Pl":3,	"CCn":"Georgia"},
		"996":	{"Geo":"mea",	"ISO3166":"KG",	"Pl":2,	"CCn":"Kyrgyzstan"},
		"998":	{"Geo":"mea",	"ISO3166":"UZ",	"Pl":2,	"CCn":"Uzbekistan"}
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
		case 3:
			if ok = l[0] <= len(nat) && len(nat) <= l[1]; ok {
				if len(nat) > l[2] {
					p, s = nat[:pl], nat[pl:l[2]]
				} else if len(p) != pl || s == "" {
					p, s = nat[:pl], nat[pl:]
				}
				return
			}
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

	// itu.int/en/ITU-T/inr/Pages/default.aspx
	// en.wikipedia.org/wiki/List_of_country_calling_codes
	// en.wikipedia.org/wiki/List_of_mobile_telephone_prefixes_by_country
	switch p, s = nat[:i.Pl], nat[i.Pl:]; cc {

	case "1": // NANPA (Jan21) en.wikipedia.org/wiki/North_American_Numbering_Plan nationalnanpa.com/area_codes/
		if len(nat) != 10 ||
			nat[0] == '0' || nat[0] == '1' || nat[1] == '9' || nat[3] == '0' || nat[3] == '1' ||
			nat[:2] == "37" || nat[:2] == "96" ||
			nat[:3] == "555" || nat[:3] == "950" || nat[:3] == "988" ||
			nat[1:3] == "11" || nat[3:8] == "55501" {
			// TODO: evaluate nat[4:6]=="11" (excluding at least 8YY)
			// TODO: evaluate nat[:3]==nat[3:6]
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
	case "211": // South Sudan (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_South_Sudan
		switch nat[0] {
		case '1', '9':
			set(2, 9)
		default:
			err()
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
	case "213": // Algeria (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Algeria
		switch nat[0] {
		case '1', '2', '3', '4':
			set(2, 8, 9)
		case '5', '6', '7', '9':
			set(2, 9)
		case '8':
			set(3, 9)
		default:
			err()
		}
	case "216": // Tunisia (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Tunisia
		switch nat[0] {
		case '2', '3', '4', '5', '7', '8', '9':
			set(2, 8)
		default:
			err()
		}
	case "218": // Libya (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Libya
		switch nat[0] {
		case '0', '1':
			err()
		default:
			set(2, 8, 9)
		}
	case "220": // Gambia (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_the_Gambia
		switch nat[0] {
		case '2', '3', '6', '7', '8', '9':
			set(1, 7)
		case '4', '5':
			set(2, 7)
		default:
			err()
		}
	case "221": // Senegal (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Senegal
		switch nat[0] {
		case '3', '7':
			set(2, 9)
		default:
			err()
		}
	case "222": // Mauritania (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Mauritania
		switch nat[0] {
		case '2', '3', '4':
			if nat[1] == '5' {
				set(3, 8)
			} else {
				set(1, 8)
			}
		case '8':
			set(1, 8)
		default:
			err()
		}
	case "223": // Mali (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Mali
		switch nat[0] {
		case '2', '4':
			set(3, 8)
		case '5', '6', '7', '8', '9':
			set(2, 8)
		default:
			err()
		}
	case "224": // Guinea (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Guinea
		switch nat[0] {
		case '2', '5', '6', '7':
			set(2, 9)
		case '3':
			set(3, 9)
		default:
			err()
		}
	case "225": // Ivory Coast (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Ivory_Coast
		switch nat[0] {
		case '0', '2':
			set(2, 10)
		case '8', '9':
			set(3, 8)
		default:
			err()
		}
	case "226": // Burkina Faso (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Burkina_Faso
		switch nat[0] {
		case '0', '5', '6', '7':
			set(2, 8)
		case '2':
			set(4, 8)
		default:
			err()
		}
	case "227": // Niger (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Niger
		switch nat[0] {
		case '2':
			set(3, 8)
		case '7', '8', '9':
			set(2, 8)
		default:
			err()
		}
	case "228": // Togo (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Togo
		switch nat[0] {
		case '2':
			set(3, 8)
		case '7', '9':
			set(2, 8)
		default:
			err()
		}
	case "229": // Benin (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Benin
		switch nat[0] {
		case '2', '4', '6', '8', '9':
			set(2, 8)
		default:
			err()
		}
	case "230": // Mauritius (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Mauritius
		switch nat[0] {
		case '2', '3', '4', '6', '7', '8', '9':
			set(2, 7)
		case '5':
			set(2, 8)
		default:
			err()
		}
	case "231": // Liberia (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Liberia
		switch nat[0] {
		case '2', '9':
			set(2, 8)
		case '3', '5', '7', '8':
			set(2, 9)
		default:
			err()
		}
	case "232": // Sierra Leone (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Sierra_Leone
		switch nat[0] {
		case '2', '3', '4', '5', '6', '7', '8':
			set(2, 8)
		default:
			err()
		}
	case "233": // Ghana (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Ghana
		switch nat[0] {
		case '2', '5':
			set(2, 9)
		case '3':
			set(2, 9)
		default:
			err()
		}
	case "234": // Nigeria (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Nigeria
		switch nat[0] {
		case '1', '2', '3', '4', '5', '6':
			set(2, 7, 8)
		case '7', '8', '9':
			if set(3, 10) || set(2, 7, 8) {
			}
		default:
			err()
		}
	case "235": // Chad (Jan21) https://en.wikipedia.org/wiki/Telephone_numbers_in_Chad
		switch nat[0] {
		case '2':
			set(3, 8)
		case '6', '7', '9':
			set(2, 8)
		default:
			err()
		}
	case "236": // Central African Republic (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_the_Central_African_Republic
		switch nat[0] {
		case '2':
			set(3, 8)
		case '7', '8':
			set(2, 8)
		default:
			err()
		}
	case "237": // Cameroon (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Cameroon
		switch nat[0] {
		case '2', '6':
			set(2, 9)
		default:
			err()
		}
	case "238": // Cape Verde (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Cape_Verde
		switch nat[0] {
		case '2', '5', '9':
			set(2, 7)
		case '3', '8':
			set(3, 7)
		default:
			err()
		}
	case "239": // Sao Tome & Principe (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_S%C3%A3o_Tom%C3%A9_and_Pr%C3%ADncipe
		switch nat[0] {
		case '2', '6', '7', '8', '9':
			set(2, 7)
		default:
			err()
		}
	case "240": // Equatorial Guinea (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Equatorial_Guinea
		switch nat[0] {
		case '2', '3', '5', '7':
			set(2, 9)
		case '8', '9':
			set(3, 9)
		default:
		}
	case "241": // Gabon (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Gabon
		switch nat[0] {
		case '1', '6', '7':
			set(2, 8)
		case '8':
			set(3, 8)
		default:
			err()
		}
	case "242": // Congo (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_the_Republic_of_the_Congo
		switch nat[0] {
		case '0':
			set(2, 9)
		case '2':
			set(4, 9)
		case '8':
			set(3, 9)
		default:
			err()
		}
	case "243": // Congo DR (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_the_Democratic_Republic_of_the_Congo
		switch nat[0] {
		case '1', '2', '3', '4', '5', '6', '7': // poor documentation
			set(2, 7, 10)
		case '8', '9':
			set(3, 9)
		default:
			err()
		}
	case "244": // Angola (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Angola
		switch nat[0] {
		case '2', '9':
			set(2, 9)
		default:
			err()
		}
	case "245": // Guinea-Bissau (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Guinea-Bissau
		switch nat[0] {
		case '4', '9':
			set(2, 9)
		default:
			err()
		}
	case "246": // Diego Garcia (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_the_British_Indian_Ocean_Territory
		switch nat[:2] {
		case "37", "38":
			set(3, 7)
		default:
			err()
		}
	case "247": // Ascension (Jan21) itu.int/dms_pub/itu-t/oth/02/02/T02020000AF0003PDFE.pdf
		switch nat[0] {
		case '0', '1', '5', '8', '9':
			set(1, 6)
		case '4':
			set(1, 5)
		case '6':
			set(2, 5)
		default:
			err()
		}
	case "248": // Seychelles (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Seychelles
		switch nat[0] {
		case '2', '4', '6':
			set(2, 7)
		case '8', '9':
			set(3, 7)
		default:
			err()
		}
	case "249": // Sudan (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Sudan
		switch nat[0] {
		case '1', '9':
			set(3, 9)
		default:
			err()
		}
	case "250": // Rwanda (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Rwanda
		switch nat[0] {
		case '2', '7', '8': // weak documentation for '8'
			set(2, 9)
		case '0': // satellite (Nov09 ITU documentation)
			set(2, 8)
		default:
			err()
		}
	case "251": // Ethiopia (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Ethiopia
		switch nat[0] {
		case '1', '2', '3', '4', '5':
			set(2, 9)
		case '9':
			set(3, 9)
		default:
			err()
		}
	case "252": // Somalia (Jan21) https://en.wikipedia.org/wiki/Telephone_numbers_in_Somalia
		switch nat[0] {
		case '0':
			err()
		default:
			if set(2, 8, 9) || set(1, 6, 7) {
			}
		}
	case "253": // Djibouti (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Djibouti
		switch nat[0] {
		case '2', '7':
			set(2, 8)
		default:
			err()
		}
	case "254": // Kenya (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Kenya
		switch nat[0] {
		case '1', '7':
			set(2, 9)
		case '2', '4', '5', '6':
			set(2, 7, 9)
		default:
			err()
		}
	case "255": // Tanzania (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Tanzania
		switch nat[0] {
		case '2', '4', '6', '7':
			set(2, 9)
		case '8', '9':
			set(3, 9)
		default:
			err()
		}
	case "256": // Uganda (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Uganda
		switch nat[0] {
		case '2':
			set(3, 9)
		case '3', '4', '7':
			set(2, 9)
		default:
			err()
		}
	case "257": // Burundi (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Burundi
		switch nat[0] {
		case '1', '2':
			set(3, 8)
		case '3', '6', '7':
			set(2, 8)
		default:
			err()
		}
	case "258": // Mozambique (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Mozambique
		switch nat[0] {
		case '2':
			set(2, 8)
		case '7', '8', '9':
			if nat[1] == '0' {
				set(3, 8, 9)
			} else {
				set(2, 9)
			}
		default:
			err()
		}
	case "260": // Zambia (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Zambia
		switch nat[0] {
		case '2', '8':
			set(3, 9)
		case '7', '9':
			if nat[1] == '0' {
				set(3, 9)
			} else {
				set(2, 9)
			}
		default:
			err()
		}
	case "261": // Madagascar (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Madagascar
		switch nat[0] {
		case '2':
			switch nat[1] {
			case '2': // satellite service
				set(2, 9)
			default:
				set(3, 9)
			}
		case '3':
			set(2, 9)
		default:
			err()
		}
	case "262": // Reunion (Jan21) itu.int/dms_pub/itu-t/oth/02/02/T020200004B0002PDFE.pdf
		switch nat[0] {
		case '2', '6', '9':
			set(3, 9)
		default:
			err()
		}
	case "263": // Zimbabwe (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Zimbabwe
		switch nat[0] {
		case '2', '3', '5', '6', '7':
			set(2, 9)
		case '8':
			if set(2, 9) || set(4, 10) {
			}
		default:
			err()
		}
	case "264": // Namibia (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Namibia
		switch nat[0] {
		case '6':
			set(2, 8, 9)
		case '8':
			if nat[0] == '0' {
				set(3, 9)
			} else {
				set(2, 9)
			}
		default:
			err()
		}
	case "265": // Malawi (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Malawi
		switch nat[0] {
		case '1':
			if set(2, 9) || set(2, 7) {
			}
		case '2', '7', '8', '9':
			set(2, 9)
		default:
			err()
		}
	case "266": // Lesotho (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Lesotho
		switch nat[0] {
		case '2', '5', '6':
			set(2, 8)
		case '8', '9':
			set(3, 8)
		default:
			err()
		}
	case "267": // Botswana (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Botswana
		switch nat[0] {
		case '2', '3', '4', '5', '6':
			set(1, 7)
		case '7':
			set(2, 8)
		case '8', '9':
			set(3, 8)
		default:
			err()
		}
	case "268": // Eswatini (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Eswatini
		switch nat[0] {
		case '2', '3', '7':
			set(2, 8)
		case '8':
			set(3, 8)
		case '9':
			set(3, 9)
		default:
			err()
		}
	case "269": // Comoros (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Comoros
		switch nat[0] {
		case '3', '4':
			set(2, 7)
		case '7', '8':
			set(3, 7)
		default:
			err()
		}
	case "27": // South Africa (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_South_Africa
		switch nat[0] {
		case '1', '2', '3', '4', '5', '7', '8', '9':
			set(2, 9)
		case '6':
			set(3, 9)
		default:
			err()
		}
	case "290": // Saint Helena & Tristan da Cunha (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Saint_Helena_and_Tristan_da_Cunha
		switch nat[0] {
		case '2':
			set(2, 5)
		case '5', '6':
			set(1, 5)
		default:
			err()
		}
	case "291": // Eritrea (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Eritrea
		switch nat[0] {
		case '1':
			set(3, 7)
		case '7', '8':
			set(1, 7)
		default:
			err()
		}
	case "297": // Aruba (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Aruba
		switch nat[:2] {
		case "28", "52", "58":
			set(2, 7)
		default:
			switch nat[0] {
			case '2', '5', '6', '7', '9':
				set(3, 7)
			default:
				err()
			}
		}
	case "298": // Faroe Islands (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_the_Faroe_Islands
		switch nat[0] {
		case '0', '1':
			err()
		default:
			set(2, 6)
		}
	case "299": // Greenland (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Greenland
		switch nat[0] {
		case '1', '2', '3', '4', '5':
			set(2, 6)
		case '6', '8', '9':
			set(1, 6)
		default:
			err()
		}

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
			// 34[616 661910 0]
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
	case "350": // Gibraltar (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Gibraltar
		switch nat[0] {
		case '2':
			set(3, 8)
		case '5':
			set(2, 8)
		default:
			err()
		}
	case "351": // Portugal (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Portugal
		switch nat[0] {
		case '2':
			switch nat[1] {
			case '1', '2':
				set(2, 9)
			default:
				set(3, 9)
			}
		case '6', '8':
			set(3, 9)
		case '3', '9':
			set(2, 9)
		default:
			err()
		}
	case "352": // Luxembourg (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Luxembourg assets.ilr.lu/telecom/Documents/ILRLU-1461723625-633.pdf
		switch nat[0] {
		case '0', '1':
			err()
		case '2':
			switch nat[1] {
			case '4', '6', '7': // TODO: add '0', '3', '8'?
				set(4, 8)
			default:
				set(2, 6)
			}
		case '4':
			set(1, 6)
		case '6':
			set(3, 9)
		default:
			set(2, 6)
		}
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
	case "354": // Iceland (3) en.wikipedia.org/wiki/Telephone_numbers_in_Iceland
		switch nat[0] {
		case '0', '1':
			err()
		case '3':
			set(3, 9)
		default:
			set(3, 7)
		}
	case "355": // Albania (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Albania
		switch nat[0] {
		case '2', '3', '4', '5':
			set(2, 8)
		case '6':
			set(2, 9)
		case '7':
			set(3, 7, 8)
		case '8':
			switch nat[1] {
			case '0':
				set(3, 6, 7)
			default:
				set(2, 8)
			}
		case '9':
			set(3, 6)
		default:
			err()
		}
	case "356": // Malta (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Malta
		switch nat[0] {
		case '2', '7', '9':
			set(2, 8)
		default:
			err()
		}
	case "357": // Cyprus (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Cyprus
		switch nat[0] {
		case '2', '5', '7', '8', '9':
			set(2, 8)
		default:
			err()
		}
	case "358": // Finland (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Finland
		switch nat[0] {
		case '0':
			err()
		case '4', '5', '9':
			set(2, 5, 10)
		default:
			switch nat[1] {
			case '0':
				set(3, 5, 10)
			default:
				set(2, 5, 10)
			}
		}
	case "359": // Bulgaria (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Bulgaria
		switch nat[:2] {
		case "43", "70", "80":
			set(3, 8)
		case "87", "88", "89", "98", "99":
			set(3, 9)
		default:
			switch nat[0] {
			case '0', '1':
				err()
			case '2':
				set(1, 8)
			default:
				set(2, 8)
			}
		}
	case "36": // Hungary (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Hungary
		switch nat[0] {
		case '0':
			err()
		case '1':
			set(1, 8)
		default:
			set(2, 8)
		}
	case "370": // Lithuania (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Lithuania
		switch nat[:2] {
		case "37", "41":
			set(2, 8)
		default:
			switch nat[0] {
			case '5':
				set(1, 8)
			case '3', '4', '6', '7', '8', '9':
				set(3, 8)
			default:
				err()
			}
		}
		switch nat[0] {
		case '5':
			set(1, 8)
		}
	case "371": // Latvia (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Latvia
		switch nat[0] {
		case '2', '6', '7':
			set(3, 8)
		case '8', '9':
			set(2, 8)
		}
	case "372": // Estonia (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Estonia
		switch nat[:2] {
		case "40", "70", "81", "82", "83", "84", "89":
			set(2, 8)
		case "80":
			set(3, 7, 10)
		case "90":
			set(3, 7)
		default:
			switch nat[0] {
			case '3', '4', '6', '7', '8':
				set(2, 7)
			case '5':
				set(2, 7, 8)
			default:
				err()
			}
		}
	case "373": // Moldova (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Moldova
		switch nat[0] {
		case '2', '3', '5', '6', '7':
			set(2, 8)
		case '8', '9':
			set(3, 8)
		default:
		}
	case "374": // Armenia (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Armenia
		switch nat[0] {
		case '2', '3':
			set(3, 8)
		case '1', '4', '5', '6', '7', '8', '9':
			set(2, 8)
		default:
			err()
		}
	case "375": // Belarus (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Belarus
		switch nat[:2] {
		case "24", "25", "29", "33", "44":
			set(2, 9)
		default:
			switch nat[0] {
			case '1', '2':
				set(4, 9)
			case '8', '9':
				set(3, 6, 10)
			default:
				err()
			}
		}
	case "376": // Andorra (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Andorra
		switch nat[:3] {
		case "180":
			set(4, 8)
		case "690":
			set(3, 9)
		default:
			switch nat[0] {
			case '1', '3', '6', '7', '8', '9':
				set(1, 6)
			default:
				err()
			}
		}
	case "377": // Monaco (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Monaco
		switch nat[0] {
		case '3', '4', '8', '9':
			set(2, 8)
		case '6':
			set(2, 9)
		default:
			err()
		}
	case "378": // San Marino (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_San_Marino
		if nat[:4] == "0549" {
			set(4, 6, 10)
		} else {
			switch nat[0] {
			case '5', '6', '7', '8', '9':
				set(2, 6, 10)
			default:
				err()
			}
		}
	case "379": // Holy See (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Vatican_City
		err()
	case "380": // Ukraine (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Ukraine
		switch nat[:3] {
		case "800", "900":
			set(3, 9, 10)
		default:
			switch nat[0] {
			case '3', '4', '5', '6', '7', '8', '9':
				set(2, 9)
			default:
				err()
			}
		}
	case "381": // Serbia (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Serbia
		switch nat[0] {
		case '1', '2', '3':
			set(2, 9)
		case '6':
			set(2, 8, 9)
		case '7', '8', '9':
			set(3, 9, 12)
		default: // unclear documentation
			err()
		}
	case "382": // Montenegro (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Montenegro
		switch nat[0] {
		case '2', '3', '4', '5', '7', '8', '9':
			set(2, 8)
		case '6': // unclear documentation
			set(2, 6, 9)
		default:
			err()
		}
	case "383": // Kosovo (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Kosovo
		switch nat[0] {
		case '2', '3', '4':
			set(2, 8, 9)
		default:
			err()
		}
	case "385": // Croatia (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Croatia
		switch nat[0] {
		case '1':
			set(1, 8)
		case '2', '3', '4', '5', '7':
			set(2, 8, 9)
		case '6':
			set(2, 6, 8)
		case '8':
			set(3, 7, 10)
		case '9':
			set(3, 9)
		default:
			err()
		}
	case "386": // Slovenia (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Slovenia
		switch nat[0] {
		case '1', '2', '3', '4', '5', '6', '7':
			set(2, 8)
		case '8', '9':
			set(2, 6, 8)
		default:
			err()
		}
	case "387": // Bosnia & Herzegovina (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Bosnia_and_Herzegovina
		switch nat[0] {
		case '3', '4', '5', '7', '9':
			set(2, 8)
		case '6':
			set(2, 8, 9)
		case '8':
			set(3, 6, 8)
		default:
			err()
		}
	case "389": // Macedonia (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_North_Macedonia
		switch nat[0] {
		case '2':
			set(1, 8)
		case '3', '4', '7':
			set(2, 8)
		case '5', '8':
			set(3, 8)
		default:
			err()
		}
	case "39": // Italy (Feb21) en.wikipedia.org/wiki/Telephone_numbers_in_Italy
		switch nat[0] {
		case '0':
			switch nat[1] {
			case '2', '6':
				set(2, 6, 11)
			default:
				// 39[081 00937102 3]
				set(3, 6, 11)
			}
		case '3':
			// 39[334 737471 02] IDT
			set(3, 9, 10)
		case '5':
			set(2, 10)
		case '8':
			set(3, 6, 10)
		default:
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
	case "420": // Czech Republic (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_the_Czech_Republic
		switch nat[0] {
		case '2':
			set(1, 9)
		case '3', '4', '5':
			set(2, 9)
		case '6', '7', '8', '9':
			set(3, 9)
		default:
			err()
		}
	case "421": // Slovakia (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Slovakia
		switch nat[0] {
		case '2':
			set(1, 9)
		case '3', '4', '5':
			set(2, 9)
		case '9': // no documentation for potential 800/900 prefixes
			set(3, 9)
		default:
			err()
		}
	case "423": // Liechtenstein (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Liechtenstein
		switch nat[0] {
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
				// 43[2732 83428 24]
				// 43[5018 70110 2]
				set(4, 7, 9)
			}
		case '6':
			switch nat[1:3] {
			case "54", "56", "58", "60":
				// 43[6601 13076 7] BT; UK +2 extra?
				// 43[6601 41040 6] BT; UK +2 extra?
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
		switch nat[0] { // extra digit allowed but truncated
		case '1':
			if nat[1] == '1' || nat[2] == '1' {
				set(3, 10, 11, 10)
			} else {
				set(4, 9, 11, 10)
			}
		case '2', '5':
			// 49[22 89789153 97] 2 extra digits (only 1 extra allowed)
			// 44[23 2393676_] GX
			// 44[28 9597281_] GX
			set(2, 10, 11, 10)
		case '3', '8', '9':
			if set(3, 10, 11, 10) || nat[:3] == "800" && set(3, 9, 10, 9) {
			}
		case '7':
			switch nat[1] {
			case '0', '6':
				set(2, 10, 11, 10)
			default:
				// 44[0 7899 828935] 0 insert; network announcement; GX completed?
				// 44[0 7412 299337] 0 insert; GX completed?
				// 44[7307 36698_]
				// 44[7738 080739 54]
				set(4, 10, 11, 10)
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
				// 48[086 539910] VZ
				err()
			} else {
				set(2, 9)
			}
		}
	case "49": // Germany (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Germany
		switch nat[:2] {
		case "15", "16", "17":
			// 49[162 4120015 31] answers with 2(3) extra digits
			// 49[152 0640359 04]
			// 49[152 5411589 90]
			// 49[152 0382342 24]
			set(3, 10, 11)
		case "30", "40", "69", "89":
			// 49[69 2030307 45] BT
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
					// 49[2289 7891539 7] VZ; 1 digit too long (network answer)
					// 49[2225 9881612 8]
					set(4, 7, 11)
				}
			}
		}

	case "500": // Falkland Islands (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_the_Falkland_Islands
		switch nat[0] {
		case '2', '3', '4', '5', '6', '7':
			set(1, 5)
		default:
			err()
		}
	case "501": // Belize (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Belize
		switch nat[0] {
		case '2', '3', '4', '5', '6', '7', '8':
			set(2, 7)
		default:
			err()
		}
	case "502": // Guatemala (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Guatemala
		switch nat[0] {
		case '1':
			switch nat[1] {
			case '8', '9':
				set(4, 11)
			default:
				err()
			}
		case '2', '3', '4', '5', '6', '7':
			set(1, 8)
		default:
			err()
		}
	case "503": // El Salvador (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_El_Salvador
		switch nat[:3] {
		case "800", "900":
			if set(3, 7) || set(3, 11) {
			}
		default:
			switch nat[0] {
			case '2', '6', '7':
				set(2, 8)
			default:
				err()
			}
		}
	case "504": // Honduras (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Honduras
		switch nat[0] {
		case '2', '3', '7', '8', '9':
			set(2, 8)
		default:
			err()
		}
	case "505": // Nicaragua (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Nicaragua
		switch nat[0] {
		case '2', '5', '7', '8':
			set(2, 8)
		default:
			err()
		}
	case "506": // Costa Rica (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Costa_Rica
		switch nat[:3] {
		case "800", "900":
			set(3, 10)
		default:
			switch nat[0] {
			case '2', '3', '4', '5', '6', '7', '8':
				set(2, 8)
			default:
				err()
			}
		}
	case "507": // Panama (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Panama
		switch nat[0] {
		case '1', '2', '3', '4', '5', '7', '8', '9':
			set(1, 7)
		case '6':
			set(2, 8)
		default:
			err()
		}
	case "508": // Saint Pierre & Miquelon (Jan21) itu.int/dms_pub/itu-t/oth/02/02/T02020000B20002PDFE.pdf
		switch nat[0] {
		case '0':
			err()
		default:
			set(0, 6)
		}
	case "509": // Haiti (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Haiti
		switch nat[0] {
		case '2', '3', '4', '9':
			set(2, 8)
		case '8':
			set(3, 8)
		default:
			err()
		}
	case "51": // Peru (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Peru
		switch nat[0] {
		case '1':
			set(1, 8)
		case '4', '5', '6', '7', '8':
			set(2, 8)
		case '9':
			set(3, 9)
		default:
			err()
		}
	case "52": // Mexico (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Mexico
		switch nat[0] {
		case '0':
			err()
		case '1': // legacy "caller-pays" prefix
			switch nat[1] {
			case '0', '1':
				err()
			default:
				switch nat[1:3] {
				case "33", "55", "56", "81":
					set(3, 11)
				default:
					set(4, 11)
				}
			}
		default:
			switch nat[:2] {
			case "33", "55", "56", "81":
				// 52[55 55888080 59] VZ
				set(2, 10)
			default:
				// 52[52 441 1074125] repeated cc?
				set(3, 10)
			}
		}
	case "53": // Cuba (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Cuba
		switch nat[0] {
		case '2', '3', '4':
			set(2, 6, 8)
		case '5':
			// 53[5 2859328 7]
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
			// 54[_05 7038132]
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
			// 57[319 5588398 1]
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
	case "590": // Guadeloupe (Jan21) itu.int/dms_pub/itu-t/oth/02/02/T02020000580002PDFE.pdf
		switch nat[0] {
		case '5', '6', '9':
			set(3, 9)
		default:
			err()
		}
	case "591": // Bolivia (Jun21) en.wikipedia.org/wiki/Telephone_numbers_in_Bolivia
		switch nat[0] {
		case '2', '3', '4', '5':
			set(1, 8)
		case '6', '7':
			set(2, 8)
		default:
			err()
		}
	case "592": // Guyana (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Guyana
		switch nat[0] {
		case '0', '1':
			err()
		default:
			set(3, 7)
		}
	case "593": // Ecuador (Jun21) en.wikipedia.org/wiki/Telephone_numbers_in_Ecuador
		switch nat[0] {
		case '2', '3', '4', '5', '6', '7':
			set(1, 8)
		case '8', '9':
			set(2, 9)
		default:
			err()
		}
	case "594": // French Guiana (Jan21) itu.int/dms_pub/itu-t/oth/02/02/T020200004C0002PDFE.pdf
		switch nat[0] {
		case '5', '6', '9':
			set(3, 9)
		default:
			err()
		}
	case "595": // Paraguay (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Paraguay
		switch nat[0] {
		case '2':
			if nat[1] == '1' {
				set(2, 9)
			} else {
				set(3, 9)
			}
		case '3', '4', '5', '6', '7', '8', '9':
			set(3, 9)
		default:
			err()
		}
	case "596": // Martinique (Jan21) itu.int/dms_pub/itu-t/oth/02/02/T02020000860002PDFE.pdf
		switch nat[0] {
		case '5', '6', '9':
			set(3, 9)
		default:
			err()
		}
	case "597": // Suriname (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Suriname
		switch nat[0] {
		case '2', '3', '4', '5':
			set(2, 6)
		case '6', '7', '8':
			set(2, 7)
		default:
			err()
		}
	case "598": // Uruguay (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Uruguay
		switch nat[0] {
		case '2':
			set(1, 8)
		case '4':
			set(3, 8)
		case '8', '9':
			if nat[1] == '0' {
				set(3, 7)
			} else {
				set(2, 8)
			}
		default:
			err()
		}
	case "599": // Caribbean Netherlands (Jan21) itu.int/dms_pub/itu-t/oth/02/02/T02020000F80003PDFE.pdf
		switch nat[0] {
		case '3', '4', '7':
			set(3, 7)
		default:
			err()
		}

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
	case "670": // Timor-Leste (Jan21) https://en.wikipedia.org/wiki/Telephone_numbers_in_East_Timor
		switch nat[0] {
		case '2', '3', '4', '8', '9':
			set(2, 7)
		case '7':
			set(2, 7, 8)
		default:
			err()
		}
	case "672": // Norfolk Island (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Norfolk_Island
		switch nat[0] {
		case '1':
			set(3, 6)
		case '2':
			set(2, 6)
		default:
			err()
		}
	case "673": // Brunei Darussalam (3) en.wikipedia.org/wiki/Telephone_numbers_in_Brunei
		switch nat[0] {
		case '2', '3', '4', '5', '7', '8':
			set(3, 7)
		default:
			err()
		}
	case "674": // Nauru (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Nauru
		switch nat[0] {
		case '4', '5', '8':
			set(3, 7)
		default:
			err()
		}
	case "675": // Papua New Guinea (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Papua_New_Guinea
		switch nat[0] {
		case '2', '3', '4', '5', '6', '9':
			set(3, 7)
		case '7', '8':
			set(2, 8)
		default:
			err()
		}
	case "676": // Tonga (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Tonga
		switch nat[0] {
		case '2', '3', '4':
			set(2, 5)
		case '5', '6', '7', '8':
			if set(3, 7) || set(2, 5) {
			}
		default:
			err()
		}
	case "677": // Solomon Islands (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Solomon_Islands
		switch nat[0] {
		case '1', '2', '3', '4', '5', '6':
			set(2, 5)
		case '7':
			set(2, 7)
		default:
			err()
		}
	case "678": // Vanuatu (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Vanuatu
		switch nat[0] {
		case '2', '3', '4', '8':
			set(2, 5)
		case '5', '7', '9':
			set(2, 7)
		default:
			err()
		}
	case "679": // Fiji (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Fiji
		switch nat[0] {
		case '0', '1':
			err()
		default:
			set(2, 7)
		}
	case "680": // Palau (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Palau
		switch nat[0] {
		case '0', '1':
			err()
		default:
			set(3, 7)
		}
	case "681": // Wallis & Futuna (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Wallis_and_Futuna
		switch nat[0] {
		case '4', '7', '8':
			set(2, 6)
		default:
			err()
		}
	case "682": // Cook Islands (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_the_Cook_Islands
		switch nat[0] {
		case '2':
			set(1, 5)
		case '3', '4', '5', '7', '8':
			set(2, 5)
		default:
			err()
		}
	case "683": // Niue (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Niue
		switch nat[0] {
		case '4', '7':
			set(0, 4)
		case '8':
			set(3, 7)
		default:
			err()
		}
	case "685": // Samoa (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Samoa
		switch nat[0] {
		case '2', '3', '4', '5', '6', '7', '8':
			set(2, 5, 7)
		default:
			err()
		}
	case "686": // Kiribati (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Kiribati
		switch nat[0] {
		case '3', '6', '7':
			set(3, 8)
		default:
			err()
		}
	case "687": // New Caledonia (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_New_Caledonia
		switch nat[:2] {
		case "55", "56", "57", "58":
			err()
		default:
			switch nat[0] {
			case '0', '1':
				err()
			default:
				set(2, 6)
			}
		}
	case "688": // Tuvalu (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Tuvalu
		switch nat[0] {
		case '2':
			set(2, 5)
		case '7':
			set(2, 7)
		case '9':
			set(2, 6)
		default:
			err()
		}
	case "689": // French Polynesia (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_French_Polynesia
		switch nat[0] {
		case '4', '8':
			set(2, 8)
		default:
			err()
		}
	case "690": // Tokelau (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Tokelau
		switch nat[0] {
		case '2', '3', '4', '5', '6', '7', '8', '9':
			set(0, 4, 5)
		default:
			err()
		}
	case "691": // Micronesia (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_the_Federated_States_of_Micronesia
		switch nat[0] {
		case '3', '9':
			set(3, 7)
		default:
			err()
		}
	case "692": // Marshall Islands (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_the_Marshall_Islands
		switch nat[0] {
		case '2', '3', '4', '5', '6':
			set(3, 7)
		default:
			err()
		}

	case "7": // Russia & Kazakhstan (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Russia en.wikipedia.org/wiki/Telephone_numbers_in_Kazakhstan
		switch nat[0] {
		case '3', '4', '6', '7', '8', '9': // '5' reserved by RU
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
			// 82[0 10 83303505] IDT; appears to strip leading 0
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
	case "850": // Korea DPR (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_North_Korea
		switch nat[0] {
		case '1':
			set(3, 10)
		case '2', '8':
			if set(4, 8) || set(3, 6) {
			}
		case '3', '4', '5', '6', '7':
			set(2, 8)
		default:
			err()
		}
	case "852": // Hong Kong (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Hong_Kong
		switch nat[0] {
		case '2', '3', '5', '6', '7':
			set(1, 8)
		case '8':
			set(3, 8, 9)
		case '9':
			set(3, 8, 11)
		default:
			err()
		}
	case "853": // Macao (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Macau
		switch nat[0] {
		case '2':
			set(3, 8)
		case '6', '8':
			set(2, 8)
		default:
			err()
		}
	case "855": // Cambodia (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Cambodia
		switch nat[:4] {
		case "1800", "1900":
			set(4, 10)
		default:
			switch nat[0] {
			case '0':
				err()
			default:
				set(2, 8, 9)
			}
		}
	case "856": // Laos (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Laos
		switch nat[:2] {
		case "20", "30":
			if set(4, 10) || set(3, 9) {
			}
		default:
			switch nat[0] {
			case '2', '3', '4', '5', '6', '7':
				set(2, 8)
			default:
				err()
			}
		}
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
	case "880": // Bangladesh (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Bangladesh
		switch nat[0] {
		case '0':
			err()
		case '1':
			set(2, 10)
		case '9':
			switch nat[1] {
			case '6':
				set(4, 10)
			default:
				set(3, 10)
			}
		default:
			set(3, 10)
		}
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
	case "886": // Taiwan (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Taiwan
		switch nat[:2] {
		case "20", "80":
			set(3, 8, 9)
		default:
			switch nat[0] {
			case '2', '3', '4', '5', '6', '7', '8':
				set(2, 8, 9)
			case '9':
				set(2, 9)
			default:
				err()
			}
		}
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
				// 91[9572 17000_]
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
	case "960": // Maldives (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_the_Maldives
		switch nat[:3] {
		case "800", "900":
			set(3, 10)
		default:
			switch nat[0] {
			case '3', '6', '7', '9':
				set(2, 7)
			default:
				err()
			}
		}
	case "961": // Lebanon (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Lebanon
		switch nat[0] {
		case '1', '3', '4', '5', '6':
			set(1, 7)
		case '7', '8', '9':
			if set(1, 7) || set(2, 8) {
			}
		default:
			err()
		}
	case "962": // Jordan (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Jordan
		switch nat[0] {
		case '2', '3', '5', '6', '8':
			set(1, 8)
		case '7':
			set(2, 9)
		default:
			err()
		}
	case "963": // Syria (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Syria
		switch nat[0] {
		case '0':
			err()
		default:
			set(2, 8, 9) // eventually set(2, 9)
		}
	case "964": // Iraq (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Iraq
		switch nat[0] {
		case '1':
			set(1, 8)
		case '2', '3', '4', '5', '6': // weak documentation
			set(2, 8)
		case '7':
			set(2, 10)
		default:
			err()
		}
	case "965": // Kuwait (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Kuwait
		switch nat[0] {
		case '1':
			set(2, 7)
		case '2', '5', '6', '9':
			set(2, 8)
		default:
			err()
		}
	case "966": // Saudi Arabia (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Saudi_Arabia
		switch nat[:3] {
		case "700":
			set(3, 8)
		case "800":
			set(3, 10)
		default:
			switch nat[0] {
			case '1', '5', '9':
				set(2, 9)
			case '8':
				set(3, 11)
			default:
				err()
			}
		}
	case "967": // Yemen (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Yemen
		switch nat[0] {
		case '1':
			set(1, 7, 8)
		case '2', '3', '4', '5', '6':
			set(1, 7)
		case '7':
			if set(2, 9) || set(1, 7) {
			}
		default:
			err()
		}
	case "968": // Oman (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Oman
		switch nat[0] {
		case '2', '7', '9':
			set(2, 8)
		default:
			err()
		}
	case "970": // Palestine (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_the_State_of_Palestine
		switch nat[0] {
		case '2', '4', '8', '9':
			set(1, 8)
		case '5':
			set(2, 9)
		default:
			err()
		}
	case "971": // United Arab Emirates (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_the_United_Arab_Emirates
		switch nat[:3] {
		case "200":
			set(3, 7)
		case "600":
			set(3, 9)
		case "800":
			set(3, 6, 7)
		default:
			switch nat[0] {
			case '2', '3', '4', '6', '7', '9':
				set(1, 8)
			case '5':
				set(2, 9)
			default:
				err()
			}
		}
	case "972": // Israel (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Israel
		switch nat[:3] {
		case "159", "170", "180", "190":
			set(4, 10)
		default:
			switch nat[0] {
			case '2', '3', '4', '8', '9':
				set(2, 8)
			case '5', '7':
				set(2, 9)
			default:
				err()
			}
		}
	case "973": // Bahrain (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Bahrain
		switch nat[0] {
		case '1', '3', '6', '7':
			set(2, 8)
		case '8', '9':
			set(3, 8)
		default:
			err()
		}
	case "974": // Qatar (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Qatar
		switch nat[0] {
		case '2':
			set(2, 7, 8)
		case '3', '4', '5', '6', '7':
			set(2, 8)
		case '8', '9':
			set(3, 7)
		default:
			err()
		}
	case "975": // Bhutan (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Bhutan
		switch nat[0] {
		case '0':
			err()
		default:
			if set(2, 8) || set(1, 7) {
			}
		}
	case "976": // Mongolia (Jan32) en.wikipedia.org/wiki/Telephone_numbers_in_Mongolia
		switch nat[0] {
		case '1', '5', '7', '8', '9':
			set(2, 8)
		default:
			err()
		}
	case "977": // Nepal (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Nepal
		switch nat[0] {
		case '1':
			set(1, 8)
		default:
			switch nat[:2] {
			case "98":
				set(2, 10)
			default:
				set(2, 8)
			}
		}
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
	case "992": // Tajikistan (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Tajikistan
		switch nat[0] {
		case '3', '4', '9':
			set(2, 9)
		default:
			err()
		}
	case "993": // Turkmenistan (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Turkmenistan
		switch nat[0] {
		case '1', '2', '3', '4', '5':
			set(3, 8)
		case '6':
			set(2, 8)
		default:
			err()
		}
	case "994": // Azerbaijan (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Azerbaijan
		switch nat[0] {
		case '1', '4', '5', '6', '7', '8', '9':
			set(2, 9)
		case '2', '3':
			set(4, 9)
		default:
			err()
		}
	case "995": // Georgia (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Georgia_(country)
		switch nat[0] {
		case '3', '4', '5', '7', '8', '9':
			set(3, 9)
		default:
			err()
		}
	case "996": // Kyrgyzstan (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Kyrgyzstan
		switch nat[0] {
		case '3', '6', '8':
			set(3, 9)
		case '2', '5', '7', '9':
			set(2, 9)
		default:
			err()
		}
	case "998": // Uzbekistan (Jan21) en.wikipedia.org/wiki/Telephone_numbers_in_Uzbekistan
		switch nat[0] {
		case '3', '6', '7', '8', '9':
			set(2, 9)
		default:
			err()
		}
	}
	return
}
