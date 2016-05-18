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
	"time"

	"github.com/juju/errors"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/weibocom/wqs/log"
)

const defaultVersion = -1

//For dup package github.com/samuel/go-zookeeper/zk log print
type logger struct {
}

func (l logger) Printf(format string, a ...interface{}) {
	log.Info("[zk] ", fmt.Sprintf(format, a...))
}

type ZkClient struct {
	*zk.Conn
}

func zkClientSetLogger(c *zk.Conn) {
	c.SetLogger(logger{})
}

func NewZkClient(servers []string) (*ZkClient, error) {
	c, _, err := zk.Connect(servers, time.Second, zkClientSetLogger)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &ZkClient{Conn: c}, nil
}

//Create a node by path with data.
func (z *ZkClient) Create(path string, data string, flags int32) error {
	_, err := z.Conn.Create(path, []byte(data), flags, zk.WorldACL(zk.PermAll))
	return err
}

//递归创建节点
func (z *ZkClient) CreateRec(zkPath string, data string, flags int32) error {
	err := z.Create(zkPath, data, flags)
	if err == zk.ErrNoNode {
		err = z.CreateRec(path.Dir(zkPath), "", flags)
		if err != nil && err != zk.ErrNodeExists {
			return err
		}
		err = z.Create(zkPath, data, flags)
	}
	return err
}

//Delete a node by path.
func (z *ZkClient) Delete(path string) error {
	return z.Conn.Delete(path, defaultVersion)
}

//递归删除
func (z *ZkClient) DeleteRec(zkPath string) error {

	err := z.Delete(zkPath)
	if err == nil {
		return nil
	}
	if err != zk.ErrNotEmpty {
		return err
	}

	children, _, err := z.Children(zkPath)
	if err != nil {
		return err
	}
	for _, child := range children {
		err = z.DeleteRec(path.Join(zkPath, child))
		if err != nil {
			return err
		}
	}

	return z.Delete(zkPath)
}

func (z *ZkClient) Set(path string, data string) error {
	_, err := z.Conn.Set(path, []byte(data), defaultVersion)
	return err
}

func (z *ZkClient) HasChildren(path string) (bool, error) {
	children, _, err := z.Conn.Children(path)
	if err != nil {
		return false, errors.Trace(err)
	}
	if len(children) == 0 {
		return false, nil
	}
	return true, nil
}
