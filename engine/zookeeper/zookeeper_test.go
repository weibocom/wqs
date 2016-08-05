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
	"os"
	"strings"
	"testing"
)

const (
	testData                = "testData"
	testSetData             = "testSetData"
	testCreatePath          = "/fortest"
	testCreateOrUpdatePath  = "/fortest/1/2"
	testCreateRecursivePath = "/fortest/1/2/3"
)

func testNewConnection() (*Conn, error) {
	addrs := os.Getenv("ZOOKEEPER_ADDR")
	if len(addrs) == 0 {
		addrs = "localhost:2181"
	}
	fmt.Printf("addr %s", addrs)
	return NewConnect(strings.Split(addrs, ","))
}

func TestConnGetSetDelete(t *testing.T) {
	conn, err := testNewConnection()
	if err != nil {
		t.Errorf("NewConnect error %v", err)
	}
	defer conn.Close()

	if err := conn.Create(testCreatePath, testData, 0); err != nil {
		t.Errorf("Create error %v", err)
	}

	if err := conn.Create(testCreatePath, testData, 0); err == nil {
		t.Errorf("Create error, should ocurr node exist error")
	} else if !IsExistError(err) {
		t.Errorf("should ocurr node exist error")
	}

	data, _, err := conn.Get(testCreatePath)
	if err != nil {
		t.Errorf("Get %s error %v", testCreatePath, err)
	}

	if string(data) != testData {
		t.Errorf("Get data %s, but expect %s", string(data), testData)
	}

	if err := conn.Set(testCreatePath, testSetData); err != nil {
		t.Fatalf("Set error %v", err)
	}

	data, _, err = conn.Get(testCreatePath)
	if err != nil {
		t.Errorf("Get %s error %v", testCreatePath, err)
	}

	if string(data) != testSetData {
		t.Errorf("Get data %s, but expect %s", string(data), testSetData)
	}

	if err := conn.Delete(testCreatePath); err != nil {
		t.Errorf("Delete %s error %v", testCreatePath, err)
	}
}

func TestRecursive(t *testing.T) {

	conn, err := testNewConnection()
	if err != nil {
		t.Fatalf("NewConnect error %v", err)
	}
	defer conn.Close()

	if err := conn.CreateRecursive(testCreateRecursivePath, testData, 0); err != nil {
		t.Fatalf("CreateRecursive error %v", err)
	}

	if err := conn.CreateRecursiveIgnoreExist(testCreateRecursivePath, "", 0); err != nil {
		t.Fatalf("CreateRecursiveIgnoreExist error %v", err)
	}

	data, _, err := conn.Get(testCreateRecursivePath)
	if err != nil {
		t.Fatalf("Get %s error %v", testCreateRecursivePath, err)
	}

	if string(data) != testData {
		t.Errorf("Get data %s, but expect %s", string(data), testData)
	}

	hasChildren, err := conn.HasChildren(testCreatePath)
	if err != nil {
		t.Fatalf("HasChildren error %v", err)
	}

	if !hasChildren {
		t.Errorf("HasChildren got wrong value %v", hasChildren)
	}

	if err := conn.CreateOrUpdate(testCreateOrUpdatePath, testData, 0); err != nil {
		t.Fatalf("CreateOrUpdate error %v", err)
	}

	data, _, err = conn.Get(testCreateOrUpdatePath)
	if err != nil {
		t.Fatalf("Get %s error %v", testCreateOrUpdatePath, err)
	}

	if string(data) != testData {
		t.Errorf("Get data %s, but expect %s", string(data), testData)
	}

	if err := conn.DeleteRecursive(testCreatePath); err != nil {
		t.Fatalf("Delete %s error %v", testCreatePath, err)
	}
}
