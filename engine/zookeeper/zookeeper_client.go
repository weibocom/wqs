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
	"container/list"
	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

type ZkClient struct {
	conn *zk.Conn
}

const defaultVersion = -1

func NewZkClient(servers []string) *ZkClient {
	zkClient := ZkClient{}
	c, _, err := zk.Connect(servers, time.Second)
	if err != nil {
		panic(err)
	}
	zkClient.conn = c
	return &zkClient
}

//创建节点
func (this *ZkClient) Create(path string, data string) bool {
	flags := int32(0)
	acl := zk.WorldACL(zk.PermAll)
	_, err := this.conn.Create(path, []byte(data), flags, acl)
	if err != nil {
		return false
	}
	return true
}

//递归创建节点
func (this *ZkClient) CreateRec(path string, data string) bool {
	flags := int32(0)
	acl := zk.WorldACL(zk.PermAll)

	path_array := strings.Split(path[1:len(path)], "/")
	var temp_path string
	for _, p := range path_array {
		temp_path += ("/" + p)
		if this.Exists(temp_path) {
			continue
		}
		this.conn.Create(temp_path, nil, flags, acl)
	}
	this.conn.Set(path, []byte(data), defaultVersion)
	return true
}

//删除节点
func (this *ZkClient) Delete(path string) bool {
	err := this.conn.Delete(path, defaultVersion)
	if err != nil {
		return false
	}
	return true
}

//递归删除
func (this *ZkClient) DeleteRec(path string) bool {
	l := list.New()
	l.PushBack(path)
	for l.Len() != 0 {
		element := l.Back()
		str, _ := element.Value.(string)
		children, _ := this.Children(str)
		if len(children) != 0 {
			for _, child := range children {
				l.PushBack(str + "/" + child)
			}
		} else {
			this.Delete(str)
			l.Remove(element)
		}
	}
	return true
}

func (this *ZkClient) Set(path string, data string) bool {
	if !this.Exists(path) {
		return false
	}
	_, err := this.conn.Set(path, []byte(data), defaultVersion)
	if err != nil {
		return false
	}
	return true
}

func (this *ZkClient) Exists(path string) bool {
	result, _, _ := this.conn.Exists(path)
	return result
}

func (this *ZkClient) Children(path string) ([]string, *zk.Stat) {
	children, stat, _ := this.conn.Children(path)
	return children, stat
}

func (this *ZkClient) Get(path string) (string, *zk.Stat) {
	value, stat, _ := this.conn.Get(path)
	data := string(value)
	return data, stat
}

func (this *ZkClient) HasChildren(path string) bool {
	children, _, _ := this.conn.Children(path)
	if len(children) == 0 {
		return false
	}
	return true
}
