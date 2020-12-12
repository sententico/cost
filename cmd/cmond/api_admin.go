package main

import (
	"fmt"
	"net/http"
	"strings"
)

type (
	// Admin ...
	Admin struct {
		Ver int
	}
)

func admin() func(int64, http.ResponseWriter, *http.Request) {
	return func(id int64, w http.ResponseWriter, r *http.Request) {
		gosrv.ServeHTTP(w, r)
	}
}

// Test method of Admin service ...
func (s *Admin) Test(args string, r *string) error {
	switch s.Ver {
	case 0:
		*r = strings.ToUpper(args)
	default:
		return fmt.Errorf("unimplemented method")
	}
	return nil
}
