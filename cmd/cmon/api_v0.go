package main

import (
	"fmt"
	"net/http"
	"net/url"
)

func api0() func(int64, http.ResponseWriter, *http.Request) {
	me := 1 // replace with method/model/accessor map
	return func(id int64, w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(fmt.Sprintf("APIv0 stub response (mm=%v id=%v)", me, id)))
	}
}

func api0VMs() func(int64, http.ResponseWriter, *http.Request) {
	me := map[string]map[string][]func(*model, url.Values, chan<- interface{}){
		"lookup": {"ec2.aws": {ec2awsLookup}, "vms.az": {}, "ce.gcs": {}},
		"sum":    {"ec2.aws": {}, "vms.az": {}, "ce.gcs": {}},
		"list":   {"ec2.aws": {}, "vms.az": {}, "ce.gcs": {}},
	}
	return func(id int64, w http.ResponseWriter, r *http.Request) {
		v := r.URL.Query()
		meth, c := v.Get("method"), 0
		mo, ok := me[meth]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		res := make(chan interface{}, 8)
		for n, al := range mo {
			if mod, ok := mMod[n]; ok {
				for _, a := range al {
					go a(mod, v, res)
				}
				c += len(al)
			}
		}
		ar := ""
		for ; c > 0; c-- {
			ar += (<-res).(string)
			// select on res & http.CloseNotifier?
			// incrementally build response per accessor result
		}
		w.Write([]byte(fmt.Sprintf("APIv0 VMs stub response (ae=%q id=%v)\n", ar, id)))
	}
}

func api0Disks() func(int64, http.ResponseWriter, *http.Request) {
	me := 1 // replace with method/model/accessor map
	return func(id int64, w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(fmt.Sprintf("APIv0 disks stub response (m=%v id=%v)\n", me, id)))
	}
}

func api0DBs() func(int64, http.ResponseWriter, *http.Request) {
	me := 1 // replace with method/model/accessor map
	return func(id int64, w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(fmt.Sprintf("APIv0 DBs stub response (m=%v id=%v)\n", me, id)))
	}
}
