package main

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/sententico/cost/cmon"
)

type (
	// API service ...
	API struct {
		Ver int
	}
)

func gorpc0() func(int64, http.ResponseWriter, *http.Request) {
	return func(id int64, w http.ResponseWriter, r *http.Request) {
		go0srv.ServeHTTP(w, r)
	}
}

// Upper (test) method of API service ...
func (s *API) Upper(args string, r *string) error {
	switch s.Ver {
	case 0:
		*r = strings.ToUpper(args)
	default:
		return fmt.Errorf("method version %v unimplemented", s.Ver)
	}
	return nil
}

// LookupVM method of API service ...
func (s *API) LookupVM(args *cmon.LookupArgs, r *string) error {
	switch authVer(args.Token, 0, s.Ver) {
	case 0:
		*r = "placeholder lookup response"
	case auNOAUTH:
		return fmt.Errorf("method access not allowed")
	default:
		return fmt.Errorf("method version %v unimplemented", s.Ver)
	}
	return nil
}
