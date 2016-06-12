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

package list

import (
	"testing"
	"unsafe"
)

func TestInit(t *testing.T) {
	node := (&Node{}).Init()
	if node.next != node {
		t.Fatalf("node.next is not node")
	}
	if node.prev != node {
		t.Fatalf("node.prev is node node")
	}
}

func TestEmpty(t *testing.T) {
	node := (&Node{}).Init()
	if !node.Empty() {
		t.Fatalf("Empty() return false, want true")
	}
}

func TestInsertAndIsSingular(t *testing.T) {
	head := (&Node{}).Init()
	node1 := (&Node{}).Init()
	node2 := (&Node{}).Init()

	node1.Insert(head)
	if head.next != node1 || head.prev != node1 || node1.next != head || node1.prev != head {
		t.Fatalf("Insert failed. head.next = %p, head.prev = %p, node1 = %p",
			head.next, head.prev, node1)
	}

	if !head.IsSingular() {
		t.Fatalf("IsSingular() failed.")
	}

	node2.Insert(head)
	if node1.prev != node2 || node1.next != head || head.prev != node1 || head.next != node2 || node2.next != node1 || node2.prev != head {
		t.Fatalf("Insert failed. head = %p, head.next = %p, head.prev = %p, node1 = %p, node2 = %p, node1.next = %p, node1.prev = %p, node2.next = %p, node2.prev = %p",
			head, head.next, head.prev, node1, node2, node1.next, node1.prev, node2.next, node2.prev)
	}

	if head.IsSingular() {
		t.Fatalf("IsSingular() failed.")
	}
}

func TestInsertToTailAndIsLast(t *testing.T) {
	head := (&Node{}).Init()
	node1 := (&Node{}).Init()
	node2 := (&Node{}).Init()

	node1.Insert(head)
	node2.InsertToTail(head)
	if head.prev != node2 || node1.next != node2 || node2.prev != node1 || node2.next != head {
		t.Fatalf("head = %p, node1 = %p node2 = %p head.prev = %p, node1.next = %p, node2.prev = %p, node2.next = %p",
			head, node1, node2, head.prev, node1.next, node2.prev, node2.next)
	}

	if !node2.IsLast(head) {
		t.Fatalf("IsLast() return false, want true")
	}
}

func TestRemove(t *testing.T) {
	head := (&Node{}).Init()
	node1 := (&Node{}).Init()

	node1.Insert(head)
	if head.Empty() {
		t.Fatalf("Empty() return true, want false")
	}
	node1.Remove()
	if !head.Empty() {
		t.Fatalf("Empty() return false, want true")
	}
}

func TestMove(t *testing.T) {
	head := (&Node{}).Init()
	node1 := (&Node{}).Init()
	node2 := (&Node{}).Init()

	node1.Insert(head)
	node2.Insert(head)

	if !node1.IsLast(head) {
		t.Fatalf("IsLast() return %t, want %t", false, true)
	}

	node1.Move(head)

	if !(node2.IsLast(head) && !node1.IsLast(head)) {
		t.Fatalf("Move() error")
	}

	node1.MoveToTail(head)
	if !node1.IsLast(head) {
		t.Fatalf("IsLast() return %t, want %t", false, true)
	}
}

func TestReplace(t *testing.T) {
	head := (&Node{}).Init()
	node1 := (&Node{}).Init()
	node2 := (&Node{}).Init()

	node1.Insert(head)
	node1.Replace(node2)

	if !node2.IsLast(head) {
		t.Errorf("Replace error.")
	}

	if head.next != node2 || head.prev != node2 || node2.next != head || node2.prev != head {
		t.Fatalf("head = %p, node1 = %p, node2 = %p, head.next = %p, head.prev = %p, node2.next = %p, node2.prev = %p",
			head, node1, node2, head.next, head.prev, node2.next, node2.prev)
	}
}

func TestJoin(t *testing.T) {
	head1 := (&Node{}).Init()
	head2 := (&Node{}).Init()
	node1 := (&Node{}).Init()
	node2 := (&Node{}).Init()

	node1.Insert(head1)
	node2.Insert(head2)

	head1.Join(head2)

	if !head2.Empty() {
		t.Fatalf("head2 should be empty.")
	}

	if node := head1.Next(); node != node2 {
		t.Fatalf("Join() error, node2 should be first of head1")
	}
}

func TestContainerOf(t *testing.T) {
	type Tnode struct {
		A    int32
		node Node
		b    int32
	}

	head := (&Node{}).Init()
	tnode := &Tnode{}
	tnode.node.Init()
	tnode.node.Insert(head)
	node := head.Next()
	if node == head {
		t.Fatalf("Insert() or Next() failed.")
	}

	tnode0 := (*Tnode)(ContainerOf(unsafe.Pointer(node), unsafe.Offsetof(tnode.node)))
	if tnode0 != tnode {
		t.Fatalf("ContainerOf failed. want:%p, get:%p", tnode, tnode0)
	}
}
