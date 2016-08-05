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

package zookeeper

import (
	"fmt"
	"path"
	"regexp"
	"time"

	"github.com/juju/errors"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/weibocom/wqs/log"
)

const (
	defaultVersion      = -1
	errorMessagePattern = `[Ff]ailed|err=`
	sessionTimeout      = time.Second
	Ephemeral           = zk.FlagEphemeral
)

//For dup package github.com/samuel/go-zookeeper/zk log print
type logger struct {
	reg *regexp.Regexp
}

func newLogger() *logger {
	return &logger{reg: regexp.MustCompile(errorMessagePattern)}
}

func (l *logger) Printf(format string, a ...interface{}) {
	message := fmt.Sprintf(format, a...)
	if len(l.reg.FindStringIndex(message)) == 0 {
		log.Info("[zk] ", message)
		return
	}
	log.Error("[zk] ", message)
}

type Conn struct {
	*zk.Conn
}

func connInit(c *zk.Conn) {
	c.SetLogger(newLogger())
}

// create a new zookeeper connection by given addrs
func NewConnect(addrs []string) (*Conn, error) {
	conn, _, err := zk.Connect(addrs, sessionTimeout, connInit)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Conn{Conn: conn}, nil
}

//Create a node by path with data.
func (c *Conn) Create(path string, data string, flags int32) error {
	_, err := c.Conn.Create(path, []byte(data), flags, zk.WorldACL(zk.PermAll))
	return err
}

//Update data of give path, if not exist create one
func (c *Conn) CreateOrUpdate(path string, data string, flags int32) error {
	err := c.CreateRecursive(path, data, flags)
	if err != nil && err == zk.ErrNodeExists {
		err = c.Set(path, data)
	}
	return err
}

//recursive create a node
func (c *Conn) CreateRecursive(zkPath string, data string, flags int32) error {
	err := c.Create(zkPath, data, flags)
	if err == zk.ErrNoNode {
		err = c.CreateRecursive(path.Dir(zkPath), "", flags)
		if err != nil && err != zk.ErrNodeExists {
			return err
		}
		err = c.Create(zkPath, data, flags)
	}
	return err
}

// recursive create a node, if it exists then omit
func (c *Conn) CreateRecursiveIgnoreExist(path string, data string, flags int32) (err error) {
	if err = c.CreateRecursive(path, data, flags); err == zk.ErrNodeExists {
		err = nil
	}
	return err
}

//Delete a node by path.
func (c *Conn) Delete(path string) error {
	return c.Conn.Delete(path, defaultVersion)
}

//递归删除
func (c *Conn) DeleteRecursive(zkPath string) error {
	err := c.Delete(zkPath)
	if err == nil {
		return nil
	}
	if err != zk.ErrNotEmpty {
		return err
	}

	children, _, err := c.Children(zkPath)
	if err != nil {
		return err
	}
	for _, child := range children {
		if err = c.DeleteRecursive(path.Join(zkPath, child)); err != nil {
			return err
		}
	}

	return c.Delete(zkPath)
}

// set data to given path
func (c *Conn) Set(path string, data string) error {
	_, err := c.Conn.Set(path, []byte(data), defaultVersion)
	return err
}

// test given path whether has sub-node
func (c *Conn) HasChildren(path string) (bool, error) {
	children, _, err := c.Conn.Children(path)
	if err != nil {
		return false, err
	}
	if len(children) == 0 {
		return false, nil
	}
	return true, nil
}

func (c *Conn) NewMutex(path string) *Mutex {
	return &Mutex{zk.NewLock(c.Conn, path, zk.WorldACL(zk.PermAll))}
}

type Mutex struct {
	lock *zk.Lock
}

func (mu *Mutex) Lock() error {
	return mu.lock.Lock()
}

func (mu *Mutex) Unlock() error {
	return mu.lock.Unlock()
}

func IsExistError(err error) bool {
	return err == zk.ErrNodeExists
}

func IsNoNode(err error) bool {
	return err == zk.ErrNoNode
}
