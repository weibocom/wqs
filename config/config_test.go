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

package config

import (
	"fmt"
	"reflect"
	"testing"
)

func TestConfig(t *testing.T) {
	config, err := NewConfigFromFile("../config.properties")
	if err != nil {
		t.Fatalf("NewConfigFromFile err : %s", err)
	}
	if config == nil {
		t.Fatalf("get nil config")
	}

	v := reflect.ValueOf(config).Elem()
	if v.Kind() != reflect.Struct {
		t.Fatalf("get config with invaild type")
	}

	for i := 0; i < v.NumField(); i++ {
		fmt.Printf("%s: %v\n", v.Type().Field(i).Name, v.Field(i).Interface())
	}

}
