package main

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/sententico/cost/cmon"
)

type (
	// API ...
	API struct {
		Ver int
	}
)

func gorpc0() func(int64, http.ResponseWriter, *http.Request) {
	return func(id int64, w http.ResponseWriter, r *http.Request) {
		go0srv.ServeHTTP(w, r)
	}
}

// Upper method of API service ...
func (s *API) Upper(args string, r *string) error {
	switch s.Ver {
	case 0:
		*r = strings.ToUpper(args)
	default:
		return fmt.Errorf("unimplemented method")
	}
	return nil
}

// AddrString method of API service ...
func (s *API) AddrString(args *cmon.Address, r *string) error {
	switch s.Ver {
	case 0:
		*r = fmt.Sprintf("%v, %v, %v %d", args.Street, args.City, args.St, args.ZIP)
	default:
		return fmt.Errorf("unimplemented method")
	}
	return nil
}
