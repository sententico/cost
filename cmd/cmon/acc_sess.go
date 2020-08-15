package main

import (
	"fmt"
	"net/url"
)

func ec2awsLookup(m *model, v url.Values, res chan<- interface{}) {
	// prepare lookup and validate v; even on error return a result on res
	id := v.Get("id")
	acc := make(chan uint32, 1)
	m.req <- modRq{0, acc}
	token := <-acc

	// perform shared access EC2 lookup
	i, _ := m.data.(ec2Model)[id]
	s := fmt.Sprintf("%v", i)

	m.rel <- token
	// create and send result
	res <- s
}
