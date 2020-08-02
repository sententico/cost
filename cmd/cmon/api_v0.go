package main

import (
	"fmt"
	"net/http"
	"net/url"
)

func api0() func(int64, http.ResponseWriter, *http.Request) {
	mm := 1 // replace with method/obj/accessor map
	return func(id int64, w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(fmt.Sprintf("APIv0 stub response (mm=%v id=%v)", mm, id)))
	}
}

func api0VMs() func(int64, http.ResponseWriter, *http.Request) {
	mm := map[string]map[string][]func(*obj, url.Values, chan<- interface{}){
		"lookup": {"ec2.aws": {ec2awsLookup}, "vms.az": {}, "ce.gcs": {}},
		"sum":    {"ec2.aws": {}, "vms.az": {}, "ce.gcs": {}},
		"list":   {"ec2.aws": {}, "vms.az": {}, "ce.gcs": {}},
	}
	return func(id int64, w http.ResponseWriter, r *http.Request) {
		v := r.URL.Query()
		m, c := v.Get("method"), 0
		om, ok := mm[m]
		if !ok {
			w.Write([]byte(fmt.Sprintf("APIv0 VMs stub response (no method)")))
			return
		}
		res := make(chan interface{}, 8)
		for on, al := range om {
			if o, ok := mObj[on]; ok {
				for _, a := range al {
					go a(o, v, res)
				}
				c += len(al)
			}
		}
		for ; c > 0; c-- {
			/* ar := */ <-res
			// select on res & http.CloseNotifier?
			// incrementally build response per accessor result
		}
		w.Write([]byte(fmt.Sprintf("APIv0 VMs stub response (om=%v id=%v)", om, id)))
	}
}

func api0Disks() func(int64, http.ResponseWriter, *http.Request) {
	mm := 1 // replace with method/obj/accessor map
	return func(id int64, w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(fmt.Sprintf("APIv0 disks stub response (m=%v id=%v)", mm, id)))
	}
}

func api0DBs() func(int64, http.ResponseWriter, *http.Request) {
	mm := 1 // replace with method/obj/accessor map
	return func(id int64, w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(fmt.Sprintf("APIv0 DBs stub response (m=%v id=%v)", mm, id)))
	}
}
