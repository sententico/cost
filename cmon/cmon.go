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
		Prefix, Suffix string
		F              func(string) string
	}
	Args string
)

// Upper ...
func (s *Test0) Upper(args *Args, r *string) error {
	*r = s.Prefix + s.F(string(*args)) + s.Suffix
	return nil
}

// Lower ...
func (s *Test0) Lower(args *Args, r *string) error {
	*r = s.Prefix + strings.ToLower(string(*args)) + s.Suffix
	return nil
}
