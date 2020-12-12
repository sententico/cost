package main

import (
	"net/http"
	"strings"
)

func gorpc0() func(int64, http.ResponseWriter, *http.Request) {
	return func(id int64, w http.ResponseWriter, r *http.Request) {
		go0srv.ServeHTTP(w, r)
	}
}

func gorpc0Upper(s string) string {
	return strings.ToUpper(s)
}
