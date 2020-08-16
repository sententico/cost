package main

import (
	"fmt"
	"net/url"
)

func ec2awsLookup(m *model, v url.Values, res chan<- interface{}) {
	// prepare lookup and validate v; even on error return a result on res
	id := v.Get("id")

	// acquire access to object model
	acc := make(chan accTok, 1)
	m.req <- modRq{0, acc}
	token := <-acc

	// perform shared access EC2 lookup, copying results before release
	i, _ := m.data[0].(ec2Model)[id]
	s := fmt.Sprintf("%v", i)

	m.rel <- token
	// after releasing access to object model, create and send result
	res <- s
}
