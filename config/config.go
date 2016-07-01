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

//主要提供配置加载功能，目前是从配置文件中进行配置加载
package config

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"github.com/juju/errors"
)

//all data is read only
type Config struct {
	ProxyId            int
	UiDir              string
	HttpPort           string
	McPort             string
	McSocketRecvBuffer int
	McSocketSendBuffer int
	MotanPort          string
	MetaDataZKAddr     string
	MetaDataZKRoot     string
	LogInfo            string
	LogDebug           string
	LogProfile         string
	LogExpire          string

	sections map[string]Section
}

func NewConfigFromBytes(data []byte) (*Config, error) {

	sections := make(map[string]Section)
	reader := bufio.NewReader(bytes.NewReader(data))

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, errors.Trace(err)
		}

		line = strings.Replace(strings.TrimSpace(line), " ", "", -1)
		if strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}

		slices := strings.SplitN(line, "=", 2)
		if len(slices) != 2 {
			continue
		}

		tokens := strings.SplitN(slices[0], ".", 2)
		if len(tokens) != 2 {
			continue
		}

		section := tokens[0]
		key := tokens[1]
		value := slices[1]

		if _, ok := sections[section]; !ok {
			sections[section] = make(Section)
		}

		sections[section][key] = value
	}

	return (&Config{sections: sections}).validate()
}

func (c *Config) String() string {

	if c == nil {
		return ""
	}
	buffer := &bytes.Buffer{}
	for name, section := range c.sections {
		for key, value := range section {
			fmt.Fprintf(buffer, "%s.%s=%s\n", name, key, value)
		}
	}
	return buffer.String()
}

func (c *Config) GetSection(name string) (Section, error) {
	if section, ok := c.sections[name]; ok {
		return section, nil
	}
	return nil, errors.NotFoundf("section:%q", name)
}

func (c *Config) GetSections() map[string]Section {
	return c.sections
}

func (c *Config) validate() (*Config, error) {

	// proxy config
	proxy, err := c.GetSection("proxy")
	if err != nil {
		return nil, errors.Trace(err)
	}
	c.ProxyId = int(proxy.GetInt64Must("id", -1))
	if c.ProxyId == -1 {
		return nil, errors.NotValidf("proxy.id")
	}

	ui, err := c.GetSection("ui")
	if err != nil {
		return nil, errors.Trace(err)
	}
	c.UiDir, err = ui.GetString("dir")
	if err != nil {
		return nil, errors.NotFoundf("ui.dir")
	}

	protocol, err := c.GetSection("protocol")
	if err != nil {
		return nil, errors.Trace(err)
	}
	c.HttpPort, err = protocol.GetString("http.port")
	if err != nil {
		return nil, errors.NotFoundf("protocol.http.port")
	}
	c.McPort, err = protocol.GetString("mc.port")
	if err != nil {
		return nil, errors.NotFoundf("protocol.mc.port")
	}

	c.McSocketRecvBuffer = int(protocol.GetInt64Must("mc.socket.buffer.recv", 4096))
	c.McSocketSendBuffer = int(protocol.GetInt64Must("mc.socket.buffer.send", 4096))

	c.MotanPort, err = protocol.GetString("motan.port")
	if err != nil {
		return nil, errors.NotFoundf("protocol.motan.port")
	}

	meta, err := c.GetSection("metadata")
	if err != nil {
		return nil, errors.Trace(err)
	}

	c.MetaDataZKAddr, err = meta.GetString("zookeeper.connect")
	if err != nil {
		return nil, errors.NotFoundf("metadata.zookeeper.connect")
	}
	c.MetaDataZKRoot, err = meta.GetString("zookeeper.root")
	if err != nil {
		return nil, errors.NotFoundf("metadata.zookeeper.root")
	}

	log, err := c.GetSection("log")
	if err != nil {
		return nil, errors.Trace(err)
	}
	c.LogInfo, err = log.GetString("info")
	if err != nil {
		return nil, errors.NotFoundf("log.info")
	}
	c.LogDebug, err = log.GetString("debug")
	if err != nil {
		return nil, errors.NotFoundf("log.debug")
	}
	c.LogProfile, err = log.GetString("profile")
	if err != nil {
		return nil, errors.NotFoundf("log.profile")
	}
	c.LogExpire, err = log.GetString("expire")
	if err != nil {
		c.LogExpire = "72h"
	}

	return c, nil
}

func NewConfigFromFile(file string) (*Config, error) {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return NewConfigFromBytes(data)
}
