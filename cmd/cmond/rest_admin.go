package main

import (
	"fmt"
	"net/http"
)

func admin() func(int64, http.ResponseWriter, *http.Request) {
	m := 1 // replace with support map as required
	return func(id int64, w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(fmt.Sprintf("admin stub response (m=%v id=%v)", m, id)))
	}
}
