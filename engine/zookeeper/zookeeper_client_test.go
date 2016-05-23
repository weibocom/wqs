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

import "testing"

var testZkList = []string{"localhost:2181"}

const (
	testZkPath  = "/fortest"
	testZkPath1 = "/fortest/1/2/3"
)

func TestZkClient(t *testing.T) {
	zk, err := NewZkClient(testZkList)
	if err != nil {
		t.Fatalf("NewZkClient err: %s", err)
	}

	err = zk.Create(testZkPath, "test", 0)
	if err != nil {
		t.Fatalf("Create err: %s", err)
	}

	data, _, err := zk.Get(testZkPath)
	if err != nil {
		t.Fatalf("Get err: %s", err)
	}
	if string(data) != "test" {
		t.Errorf("Get wrong data:%s, expect:test", string(data))
	}

	_, _, err = zk.Children("/")
	if err != nil {
		t.Fatalf("Children err: %s", err)
	}

	err = zk.Delete(testZkPath)
	if err != nil {
		t.Fatalf("Delete err: %s", err)
	}
}

func TestZkRecursiveOperations(t *testing.T) {
	zk, err := NewZkClient(testZkList)
	if err != nil {
		t.Fatalf("NewZkClient err: %s", err)
	}

	err = zk.CreateRec(testZkPath1, "test", 0)
	if err != nil {
		t.Fatalf("Create err: %s", err)
	}

	data, _, err := zk.Get(testZkPath1)
	if err != nil {
		t.Fatalf("Get err: %s", err)
	}
	if string(data) != "test" {
		t.Errorf("Get wrong data:%s, expect:test", string(data))
	}

	err = zk.DeleteRec(testZkPath)
	if err != nil {
		t.Fatalf("Delete err: %s", err)
	}

	exist, _, err := zk.Exists(testZkPath)
	if err != nil {
		t.Fatalf("Exists err: %s", err)
	}
	if exist {
		t.Fatalf("Exists wrong.")
	}
}
