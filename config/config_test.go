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
	"testing"
)

func TestConfigFromFile(t *testing.T) {
	config, err := NewConfigFromFile("../config.properties")
	if err != nil {
		t.Fatalf("NewConfigFromFile err : %v", err)
	}
	t.Logf("config:\n%s\n", config.String())
}

func TestGetSection(t *testing.T) {
	config, err := NewConfigFromFile("../config.properties")
	if err != nil {
		t.Fatalf("NewConfigFromFile err : %s", err)
	}
	if config == nil {
		t.Fatalf("get nil config")
	}
	sec, err := config.GetSection("metrics")
	if err != nil || sec == nil {
		t.FailNow()
	}
}

func TestGetSections(t *testing.T) {
	config, err := NewConfigFromFile("../config.properties")
	if err != nil {
		t.Fatalf("NewConfigFromFile err : %s", err)
	}
	if config == nil {
		t.Fatalf("config is nil, want is not nil")
	}
	sections := config.GetSections()
	if len(sections) != len(config.sections) {
		t.Fatalf("len(sections) != len(config.sections)")
	}
}
