package cmon

type (
	// awsService settings
	awsService struct {
		Options                    string
		SavPlan                    string
		SavCov, SpotDisc, UsageAdj float32
		Accounts                   map[string]map[string]float32
	}
	// datadogService settings
	datadogService struct {
		Options        string
		APIKey, AppKey string
	}

	// MonSettings are composite settings for the cloud monitor
	MonSettings struct {
		Options         string
		Unit, Port      string
		WorkDir, BinDir string
		Models          map[string]string
		AWS             awsService
		Datadog         datadogService
	}
)

// Test0 ...
type (
	Test0 struct {
		M func(string, interface{}, interface{}) error
	}
)

// Upper ...
func (s *Test0) Upper(args string, r *string) error {
	return s.M("Test0.Upper", args, r)
}

// Lower ...
func (s *Test0) Lower(args string, r *string) error {
	return s.M("Test0.Lower", args, r)
}
