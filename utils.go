package goflows

import (
	"reflect"
)

func inSlice(val interface{}, array interface{}) bool {
	if reflect.TypeOf(array).Kind() == reflect.Slice {
		s := reflect.ValueOf(array)

		for i := 0; i < s.Len(); i++ {
			if reflect.DeepEqual(val, s.Index(i).Interface()) {
				return true
			}
		}
	}

	return false
}
