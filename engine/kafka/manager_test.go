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

package kafka

import (
	"fmt"
	"testing"
)

func TestAssignment0(t *testing.T) {
	brokersList := []int32{0}
	_, err := assignReplicasToBrokers(brokersList, 4, 2, -1, -1)
	if err == nil {
		t.Errorf("should ocurr error.")
	}
}

func TestAssignment1(t *testing.T) {
	brokersList := []int32{0}
	assignment, err := assignReplicasToBrokers(brokersList, 4, 1, -1, -1)
	if err != nil {
		t.Errorf("unexpect error: %s", err)
	}
	fmt.Printf("broker:1 partiton:1 replication:1 :\n\t%v\n", assignment)
}

func TestAssignment2(t *testing.T) {
	brokersList := []int32{0, 1, 2, 3}
	assignment, err := assignReplicasToBrokers(brokersList, 4, 1, -1, -1)
	if err != nil {
		t.Errorf("unexpect error: %s", err)
	}
	fmt.Printf("broker:4 partiton:1 replication:1 :\n\t%v\n", assignment)
}

func TestAssignment3(t *testing.T) {
	brokersList := []int32{0, 1, 2, 3}
	assignment, err := assignReplicasToBrokers(brokersList, 4, 4, -1, -1)
	if err != nil {
		t.Errorf("unexpect error: %s", err)
	}
	fmt.Printf("broker:4 partiton:4 replication:4 :\n\t%v\n", assignment)
}

func TestAssignment4(t *testing.T) {
	brokersList := []int32{0, 1, 2, 3}
	assignment, err := assignReplicasToBrokers(brokersList, 4, 4, -1, -1)
	if err != nil {
		t.Errorf("unexpect error: %s", err)
	}
	fmt.Printf("prev broker:4 partiton:4 replication:4 :\n\t%v\n", assignment)

	newAssignment, err := assignReplicasToBrokers(brokersList, 4,
		4, assignment["0"][0], int32(len(assignment)))
	if err != nil {
		t.Errorf("unexpect error: %s", err)
	}
	fmt.Printf("new broker:4 partiton:2 replication:4 :\n\t%v\n", newAssignment)

	for partition, assign := range newAssignment {
		if len(assign) != 4 {
			t.Errorf("new replication assignment %v", newAssignment)
		}
		assignment[partition] = assign
	}
	fmt.Printf("merge broker:4 partiton:6 replication:4 :\n\t%v\n", assignment)
}
