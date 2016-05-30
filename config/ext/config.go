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
package ext

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

type IConfig interface {
	Load() (m map[string]Section, err error)
	LastModify() int64
}

type Section map[string]string

type Config struct {
	icfg       IConfig
	m          map[string]Section
	ch         chan map[string]Section
	rw         *sync.RWMutex
	ttl        time.Duration
	lastModify int64
}

func NewConfig(file string, refresh time.Duration) (cfg *Config, err error) {
	icfg := &fileConfig{dir: file}
	m, err := icfg.Load()
	if err != nil {
		return nil, err
	}
	cfg = &Config{
		icfg: icfg,
		m:    m,
		rw:   new(sync.RWMutex),
		ch:   make(chan map[string]Section, 16),
	}
	return
}

func NewConfigFromEtcd(addr string) (cfg *Config, err error) {
	// TODO
	return
}

func (c *Config) GetSection(name string) (sec Section, err error) {
	c.rw.RLock()
	sec, ok := c.m[name]
	if !ok {
		err = fmt.Errorf("Cannot find section[%s]", name)
	}
	c.rw.RUnlock()
	return
}

func (c *Config) GetSections() map[string]Section {
	c.rw.RLock()
	defer c.rw.RUnlock()
	return c.m
}

func (c *Config) DumpConf() []byte {
	var dump []byte
	bf := bytes.NewBuffer(dump)
	for sec := range c.m {
		fmt.Fprintf(bf, "[%s]\n", sec)
		for k, v := range c.m[sec] {
			fmt.Fprintf(bf, "%s = %s\n", k, v)
		}
		fmt.Fprintln(bf, "")
	}
	return bf.Bytes()
}

func (c *Config) PeriodUpdate() {
	if c.ttl < time.Second*1 {
		return
	}
	tk := time.NewTicker(c.ttl)
	defer tk.Stop()

	go func() {
		for {
			select {
			case <-tk.C:
				m, err := c.icfg.Load()
				if err != nil {
				} else {
					c.ch <- m
				}
			}
		}
	}()

	var m map[string]Section
	for {
		select {
		case m = <-c.ch:
			c.rw.Lock()
			c.m = m
			c.lastModify = time.Now().Unix()
			c.rw.Unlock()
		}
	}
}

func (s Section) GetString(key string) (val string, err error) {
	var ok bool
	val, ok = s[key]
	if !ok {
		err = fmt.Errorf("Cannot find key[%s]", key)
	}
	return
}

func (s Section) GetInt64(key string) (val int64, err error) {
	str, ok := s[key]
	if !ok {
		return 0, fmt.Errorf("Cannot find key[%s]", key)
	}
	val, err = strconv.ParseInt(str, 10, 64)
	return
}

func (s Section) GetFloat64(key string) (val float64, err error) {
	str, ok := s[key]
	if !ok {
		return 0.0, fmt.Errorf("Cannot find key[%s]", key)
	}
	val, err = strconv.ParseFloat(str, 64)
	return
}

func (s Section) GetBool(key string) (val bool, err error) {
	str, ok := s[key]
	if !ok {
		return false, fmt.Errorf("Cannot find key[%s]", key)
	}
	switch strings.ToUpper(str) {
	case "TRUE":
		val = true
	case "FALSE":
		val = false
	default:
		err = fmt.Errorf("Not supported bool val[%s]", str)
	}
	return
}

func (s Section) GetStringMust(key, defaultVal string) (val string) {
	val, err := s.GetString(key)
	if err != nil {
		val = defaultVal
	}
	return
}

func (s Section) GetInt64Must(key string, defaultVal int64) (val int64) {
	val, err := s.GetInt64(key)
	if err != nil {
		val = defaultVal
	}
	return
}

func (s Section) GetBoolMust(key string, defaultVal bool) (val bool) {
	val, err := s.GetBool(key)
	if err != nil {
		val = defaultVal
	}
	return
}

func (s Section) GetFloat64Must(key string, defaultVal float64) (val float64) {
	val, err := s.GetFloat64(key)
	if err != nil {
		val = defaultVal
	}
	return
}
