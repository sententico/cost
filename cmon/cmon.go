package cmon

import "strings"

// Test0 ...
type Test0 struct {
	S string
}

// Upper ...
func (t *Test0) Upper(args *Test0, r *string) error {
	*r = strings.ToUpper(args.S)
	return nil
}

// Lower ...
func (t *Test0) Lower(args *Test0, r *string) error {
	*r = strings.ToLower(args.S)
	return nil
}
