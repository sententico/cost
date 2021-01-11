package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/sententico/cost/cmon"
)

type (
	// API service ...
	API struct {
		Ver int
	}
)

func gorpc0() func(int64, http.ResponseWriter, *http.Request) {
	return func(id int64, w http.ResponseWriter, r *http.Request) {
		go0srv.ServeHTTP(w, r)
	}
}

// Upper (test) method of API service ...
func (s *API) Upper(args string, r *string) (err error) {
	var weain io.WriteCloser
	var weaout io.ReadCloser
	defer func() {
		if re := recover(); re != nil {
			err = re.(error)
		}
		weain.Close()
		weaout.Close()
	}()

	switch s.Ver {
	case 0:
		// s := []string{args}
		// if weain, weaout, err = weaselCmd.new("up.test", []interface{}{s}); err != nil {
		// 	return fmt.Errorf("couldn't release weasel: %v", err)
		// }
		// weain.Close()
		// if err = json.NewDecoder(weaout).Decode(&s); err != nil {
		//	return fmt.Errorf("error decoding response: %v", err)
		// }
		// *r = s[0]

		// *r = strings.ToUpper(args)

		a, n := map[string]string{
			"Channel": "#telecom-fraud",
			"Text":    args,
		}, 0
		if weain, weaout, err = weaselCmd.new("hook.slack", []interface{}{a}); err != nil {
			return fmt.Errorf("couldn't release weasel: %v", err)
		}
		weain.Close()
		if err = json.NewDecoder(weaout).Decode(&n); err != nil {
			return fmt.Errorf("error decoding response: %v", err)
		} else if n != 1 {
			return fmt.Errorf("response code: %v", n)
		}
		*r = "ok"
	default:
		return fmt.Errorf("method version %v unimplemented", s.Ver)
	}
	return
}

// LookupVM method of API service ...
func (s *API) LookupVM(args *cmon.LookupArgs, r *string) (err error) {
	defer func() {
		if re := recover(); re != nil {
			err = re.(error)
		}
	}()

	switch authVer(args.Token, 0, s.Ver) {
	case 0:
		*r = "placeholder lookup response"
	case auNOAUTH:
		return fmt.Errorf("method access not allowed")
	default:
		return fmt.Errorf("method version %v unimplemented", s.Ver)
	}
	return
}

// Series method of API service ...
func (s *API) Series(args *cmon.SeriesArgs, r *cmon.SeriesRet) (err error) {
	defer func() {
		if re := recover(); re != nil {
			err = re.(error)
		}
	}()

	switch authVer(args.Token, 0, s.Ver) {
	case 0:
		var c chan *cmon.SeriesRet
		if c, err = seriesExtract(args.Metric, args.Span, args.Recent, args.Truncate); err != nil {
			return
		}
		*r = *<-c
	case auNOAUTH:
		return fmt.Errorf("method access not allowed")
	default:
		return fmt.Errorf("method version %v unimplemented", s.Ver)
	}
	return
}

// Stream method of API service ...
func (s *API) Stream(args *cmon.StreamArgs, r *[][]string) (err error) {
	defer func() {
		if re := recover(); re != nil {
			err = re.(error)
		}
	}()

	switch authVer(args.Token, 0, s.Ver) {
	case 0:
		var c chan []string
		if c, err = streamExtract(args.Model, args.Items); err != nil {
			return
		}
		*r = make([][]string, 0, args.Items)
		for s := range c {
			*r = append(*r, s)
		}
	case auNOAUTH:
		return fmt.Errorf("method access not allowed")
	default:
		return fmt.Errorf("method version %v unimplemented", s.Ver)
	}
	return
}

// StreamCUR method of API service ...
func (s *API) StreamCUR(args *cmon.StreamCURArgs, r *[][]string) (err error) {
	defer func() {
		if re := recover(); re != nil {
			err = re.(error)
		}
	}()

	switch authVer(args.Token, 0, s.Ver) {
	case 0:
		var c chan []string
		if c, err = curawsExtract(args.From, args.To, args.Units, args.Items, args.Truncate); err != nil {
			return
		}
		*r = make([][]string, 0, args.Items)
		for s := range c {
			*r = append(*r, s)
		}
	case auNOAUTH:
		return fmt.Errorf("method access not allowed")
	default:
		return fmt.Errorf("method version %v unimplemented", s.Ver)
	}
	return
}
