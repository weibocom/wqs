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

package log

import (
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync/atomic"
	"time"
)

const defaultExpire = 72 * time.Hour

type none struct{}

type Cleaner struct {
	dirs     map[string]none
	duration time.Duration
	expire   time.Duration
	stopping int32
	stopCh   chan none
}

func NewCleaner(expire string, files ...string) *Cleaner {

	expTime, err := time.ParseDuration(expire)
	if err != nil {
		expTime = defaultExpire
	}

	cleaner := &Cleaner{
		dirs:     make(map[string]none),
		duration: expTime / 2,
		expire:   expTime,
		stopCh:   make(chan none),
	}

	for _, file := range files {
		dir := path.Dir(file)
		cleaner.dirs[dir] = none{}
	}
	return cleaner
}

func (c *Cleaner) Start() {

	if !atomic.CompareAndSwapInt32(&c.stopping, 0, 1) {
		return
	}

	go c.eventLoop()
}

func (c *Cleaner) Stop() {

	if !atomic.CompareAndSwapInt32(&c.stopping, 1, 2) {
		return
	}
	close(c.stopCh)
}

func (c *Cleaner) eventLoop() {

	c.check(time.Now())
	ticker := time.NewTicker(c.duration)

	for {
		select {
		case <-c.stopCh:
			ticker.Stop()
			return
		case now := <-ticker.C:
			c.check(now)
		}
	}
}

func (c *Cleaner) check(now time.Time) {

	for dir := range c.dirs {
		files, err := ioutil.ReadDir(dir)
		if err != nil {
			continue
		}

		for _, file := range files {
			if strings.Contains(file.Name(), ".log.") &&
				now.Sub(file.ModTime()) > c.expire && !file.IsDir() {
				// remove expired files
				os.Remove(path.Join(dir, file.Name()))
			}
		}
	}
}
