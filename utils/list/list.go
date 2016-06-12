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

import "unsafe"

type Node struct {
	prev *Node
	next *Node
}

//Insert node at head of list `head`
func (n *Node) Insert(head *Node) {
	head.next.prev = n
	n.next = head.next
	n.prev = head
	head.next = n
}

//Insert node at tail of list `head`
func (n *Node) InsertToTail(head *Node) {
	head.prev.next = n
	n.prev = head.prev
	n.next = head
	head.prev = n
}

//Replace old entry `n` by new one `node`
func (n *Node) Replace(node *Node) {
	node.next = n.next
	node.next.prev = node
	node.prev = n.prev
	node.prev.next = node
}

//Remove node by itself
func (n *Node) Remove() {
	n.prev.next = n.next
	n.next.prev = n.prev
}

//Move node from old entry to head of `head`
func (n *Node) Move(head *Node) {
	n.Remove()
	n.Init().Insert(head)
}

//Move node from old entry to tail of `head`
func (n *Node) MoveToTail(head *Node) {
	n.Remove()
	n.Init().InsertToTail(head)
}

//Tests whether `n` is the last entry in list `head`
func (n *Node) IsLast(head *Node) bool {
	return n.next == head
}

//Tests whether a list `n` is empty
func (n *Node) Empty() bool {
	return n.next == n
}

//Tests whether a list `n` has just one entry.
func (n *Node) IsSingular() bool {
	return !n.Empty() && n.prev == n.next
}

//List `n` join list head, add `head` to head of `n`
func (n *Node) Join(head *Node) {
	if !head.Empty() {
		head.next.prev = n
		n.next = head.next
		head.prev.next = n.next
		n.next.prev = head.prev
		head.Init()
	}
}

//Get next entry of node `n`
func (n *Node) Next() *Node {
	return n.next
}

//Init Node internal param
func (n *Node) Init() *Node {
	n.prev = n
	n.next = n
	return n
}

func ContainerOf(in unsafe.Pointer, offset uintptr) unsafe.Pointer {
	return unsafe.Pointer(uintptr(in) - offset)
}
