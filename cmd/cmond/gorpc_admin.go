package main

import (
	"net/http"
)

func admin() func(int64, http.ResponseWriter, *http.Request) {
	return func(id int64, w http.ResponseWriter, r *http.Request) {
		gosrv.ServeHTTP(w, r)
	}
}
