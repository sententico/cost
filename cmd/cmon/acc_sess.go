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
	var s string
	if inst, _ := m.data[0].(*ec2Model).Inst[id]; inst != nil {
		s = fmt.Sprintf("%v", *inst)
	}
	m.rel <- token

	// after releasing access to object model, create and send result
	res <- s
}

func ebsawsLookup(m *model, v url.Values, res chan<- interface{}) {
	// prepare lookup and validate v; even on error return a result on res
	id := v.Get("id")

	// acquire access to object model
	acc := make(chan accTok, 1)
	m.req <- modRq{0, acc}
	token := <-acc

	// perform shared access EC2 lookup, copying results before release
	var s string
	if vol, _ := m.data[0].(*ebsModel).Vol[id]; vol != nil {
		s = fmt.Sprintf("%v", *vol)
	}
	m.rel <- token

	// after releasing access to object model, create and send result
	res <- s
}

func rdsawsLookup(m *model, v url.Values, res chan<- interface{}) {
	// prepare lookup and validate v; even on error return a result on res
	id := v.Get("id")

	// acquire access to object model
	acc := make(chan accTok, 1)
	m.req <- modRq{0, acc}
	token := <-acc

	// perform shared access EC2 lookup, copying results before release
	var s string
	if db, _ := m.data[0].(*rdsModel).DB[id]; db != nil {
		s = fmt.Sprintf("%v", *db)
	}
	m.rel <- token

	// after releasing access to object model, create and send result
	res <- s
}
