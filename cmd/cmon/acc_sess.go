package main

import (
	"fmt"
	"net/url"
)

func ec2awsLookup(o *obj, v url.Values, res chan<- interface{}) {
	// prepare lookup and validate v; even on error return a result on res
	id := v.Get("id")
	acc := make(chan uint32, 1)
	o.req <- objRq{0, acc}
	token := <-acc

	// perform shared access EC2 lookup
	i, _ := o.data.(objEC2)[id]
	s := fmt.Sprintf("%v", i)

	o.rel <- token
	// create and send result
	res <- s
}
