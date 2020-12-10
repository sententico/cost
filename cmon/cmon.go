package cmon

import "strings"

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

	// monSettings are composite settings for the cloud monitor
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
type Test0 struct {
	S string
}

// Upper ...
func (t *Test0) Upper(args *Test0, r *string) error {
	*r = strings.ToUpper(args.S)
	return nil
}

// Lower ...
func (t *Test0) Lower(args *Test0, r *string) error {
	*r = strings.ToLower(args.S)
	return nil
}
