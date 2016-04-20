/*
Copyright 2009-2016 Weibo, Inc.

All files licensed under the Apache License, Version 2.0 (the "License");
you may not use these files except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package utils

import (
	"strconv"

	"github.com/juju/errors"
)

func GenTestMessage(length int) string {
	message := ""
	for i := 0; i < length; i++ {
		message += "T"
	}
	return message
}

func GetIntFromArgs(args map[string]interface{}, arg string, defaultVar int) (int, error) {
	if n := args[arg]; n != nil {
		v, err := strconv.Atoi(n.(string))
		return v, errors.Trace(err)
	}
	return defaultVar, nil
}
