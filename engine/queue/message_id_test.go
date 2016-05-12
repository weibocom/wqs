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

package queue

import (
	"fmt"
	"testing"
	"time"
)

func TestMessageID(t *testing.T) {
	genor := newIDGenerator(0xEE)
	fmt.Printf("time:%x, base %x\n", time.Now().UnixNano()/1e6, baseTime)
	fmt.Printf("time: %x\n", time.Now().UnixNano()/1e6-baseTime)
	for i := 0; i < 10; i++ {
		fmt.Printf("message ID : %x\n", genor.Get())
	}
	fmt.Printf("time: %x\n", time.Now().UnixNano()/1e6-baseTime)
	for i := 0; i < 10; i++ {
		time.Sleep(128 * time.Millisecond)
		fmt.Printf("message ID : %x\n", genor.Get())
	}
}

//防止编译器在test时，内联优化
func dummy(id uint64) {
}

func BenchmarkMessageID(b *testing.B) {
	genor := newIDGenerator(0xEE)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := genor.Get()
		dummy(id)
	}
}

func BenchmarkMessageIDParallel(b *testing.B) {
	genor := newIDGenerator(0xEE)
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := genor.Get()
			dummy(id)
		}
	})
}
