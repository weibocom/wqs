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
	"regexp"
	"strconv"

	"github.com/juju/errors"
)

type Section map[string]string

func (s Section) GetString(key string) (string, error) {
	if value, ok := s[key]; ok {
		return value, nil
	}
	return "", errors.NotFoundf("key:%q", key)
}

func (s Section) GetInt64(key string) (int64, error) {
	if value, ok := s[key]; ok {
		return strconv.ParseInt(value, 10, 64)
	}
	return 0, errors.NotFoundf("key:%q", key)
}

func (s Section) GetFloat64(key string) (float64, error) {
	if value, ok := s[key]; ok {
		return strconv.ParseFloat(value, 64)
	}
	return 0.0, errors.NotFoundf("key:%q", key)
}

func (s Section) GetBool(key string) (bool, error) {
	if value, ok := s[key]; ok {
		return strconv.ParseBool(value)
	}
	return false, errors.NotFoundf("key:%q", key)
}

func (s Section) GetStringMust(key, defaultVal string) string {
	value, err := s.GetString(key)
	if err != nil {
		value = defaultVal
	}
	return value
}

func (s Section) GetInt64Must(key string, defaultVal int64) int64 {
	value, err := s.GetInt64(key)
	if err != nil {
		value = defaultVal
	}
	return value
}

func (s Section) GetBoolMust(key string, defaultVal bool) bool {
	value, err := s.GetBool(key)
	if err != nil {
		value = defaultVal
	}
	return value
}

func (s Section) GetFloat64Must(key string, defaultVal float64) float64 {
	value, err := s.GetFloat64(key)
	if err != nil {
		value = defaultVal
	}
	return value
}

func (s Section) GetDupByPattern(pattern string) Section {
	reg := regexp.MustCompile(pattern)
	section := make(Section)
	for key, value := range s {
		if reg.MatchString(key) {
			section[key] = value
		}
	}
	return section
}
