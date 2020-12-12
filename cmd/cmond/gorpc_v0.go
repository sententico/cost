package main

import (
	"fmt"
	"net/http"
	"strings"
)

func gorpc0() func(int64, http.ResponseWriter, *http.Request) {
	return func(id int64, w http.ResponseWriter, r *http.Request) {
		go0srv.ServeHTTP(w, r)
	}
}

func gorpcTest0(method string, args interface{}, r interface{}) error {
	switch method {
	case "Test0.Upper":
		*r.(*string) = strings.ToUpper(args.(string))
	case "Test0.Lower":
		*r.(*string) = strings.ToLower(args.(string))
	default:
		return fmt.Errorf("unimplemented method")
	}
	return nil
}
