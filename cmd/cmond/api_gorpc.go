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
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	switch s.Ver {
	case 0:
		var stdin io.WriteCloser
		var stdout io.ReadCloser
		if stdin, stdout, err = weasel("up.test"); err != nil {
			return fmt.Errorf("couldn't release weasel: %v", err)
		}
		s := []string{args}
		if err = json.NewEncoder(stdin).Encode(&s); err != nil {
			return fmt.Errorf("error encoding request")
		}
		fmt.Fprintln(stdin)
		stdin.Close()
		if err = json.NewDecoder(stdout).Decode(&s); err != nil {
			return fmt.Errorf("error decoding response: %v", err)
		}
		*r = s[0]
		// *r = strings.ToUpper(args)
	default:
		return fmt.Errorf("method version %v unimplemented", s.Ver)
	}
	return
}

// LookupVM method of API service ...
func (s *API) LookupVM(args *cmon.LookupArgs, r *string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
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
func (s *API) Series(args *cmon.SeriesArgs, r *map[string][]float64) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	switch authVer(args.Token, 0, s.Ver) {
	case 0:
		var c chan map[string][]float64
		if c, err = seriesExtract(args.Metric, args.Span, args.Recent, args.Truncate); err != nil {
			return
		}
		*r = <-c
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
		if r := recover(); r != nil {
			err = r.(error)
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
