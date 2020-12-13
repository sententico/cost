package main

import (
	"fmt"
	"net/http"

	"github.com/sententico/cost/cmon"
)

type (
	// Admin service ...
	Admin struct {
		Ver int
	}
)

const (
	auREAD = 1 << iota
	auWRITE
	auNOAUTH = -1
)

func admin() func(int64, http.ResponseWriter, *http.Request) {
	return func(id int64, w http.ResponseWriter, r *http.Request) {
		gosrv.ServeHTTP(w, r)
	}
}

func authVer(tok string, access uint, grant int) int {
	if tok == "placeholder_access_token" {
		return grant
	}
	return auNOAUTH
}

// Auth method of Admin service ...
func (s *Admin) Auth(args *cmon.AuthArgs, r *string) error {
	switch s.Ver {
	case 0:
		*r = "placeholder_access_token"
	default:
		return fmt.Errorf("method version %v unimplemented", s.Ver)
	}
	return nil
}
