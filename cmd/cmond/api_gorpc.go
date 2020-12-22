package main

import (
	"encoding/json"
	"fmt"
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
func (s *API) Upper(args string, r *string) error {
	switch s.Ver {
	case 0:
		if stdin, stdout, err := weasel("up.test"); err != nil {
			return fmt.Errorf("couldn't release weasel: %v", err)
		} else {
			s := []string{args}
			json.NewEncoder(stdin).Encode(&s)
			fmt.Fprintln(stdin)
			stdin.Close()
			json.NewDecoder(stdout).Decode(&s)
			*r = s[0]
		}
		// *r = strings.ToUpper(args)
	default:
		return fmt.Errorf("method version %v unimplemented", s.Ver)
	}
	return nil
}

// LookupVM method of API service ...
func (s *API) LookupVM(args *cmon.LookupArgs, r *string) error {
	switch authVer(args.Token, 0, s.Ver) {
	case 0:
		*r = "placeholder lookup response"
	case auNOAUTH:
		return fmt.Errorf("method access not allowed")
	default:
		return fmt.Errorf("method version %v unimplemented", s.Ver)
	}
	return nil
}

// Series method of API service ...
func (s *API) Series(args *cmon.SeriesArgs, r *map[string][]float64) error {
	switch authVer(args.Token, 0, s.Ver) {
	case 0:
		c, err := seriesExtract(args.Metric, args.Span, args.Recent, args.Truncate)
		if err != nil {
			return err
		}
		*r = <-c
	case auNOAUTH:
		return fmt.Errorf("method access not allowed")
	default:
		return fmt.Errorf("method version %v unimplemented", s.Ver)
	}
	return nil
}

// StreamCUR method of API service ...
func (s *API) StreamCUR(args *cmon.StreamCURArgs, r *[][]string) error {
	switch authVer(args.Token, 0, s.Ver) {
	case 0:
		c, err := curawsExtract(args.From, args.To, args.Units, args.Items, args.Truncate)
		if err != nil {
			return err
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
	return nil
}
