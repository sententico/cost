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

// API argument/return types...
type (
	// AuthArgs ...
	AuthArgs struct {
		ID   string // identification token
		Hash string // SHA256 of RFC3339 GMT (YYYY-MM-DDThh:mm) concatenated with secret token
	}

	// LookupArgs ...
	LookupArgs struct {
		Token string // Admin.Auth access token (renew hourly to avoid expiration)
		Key   string // lookup key
	}

	// SeriesArgs ...
	SeriesArgs struct {
		Token     string  // Admin.Auth access token (renew hourly to avoid expiration)
		Metric    string  // metric identifier
		History   int     // hours in series to return
		Recent    int     // recent hours
		Threshold float64 // minimum ...
	}
	// SeriesRet ...
	SeriesRet map[string][]float64

	// StreamCURArgs ...
	StreamCURArgs struct {
		Token     string  // Admin.Auth access token (renew hourly to avoid expiration)
		From      int32   // from hour
		To        int32   // to hour
		Items     int     // maximum line items
		Threshold float32 // minimum line item cost
	}
)
