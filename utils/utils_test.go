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

package utils

import (
	"testing"
)

func dummy(s string) {

}

func BenchmarkMapTest01(b *testing.B) {
	dmap := make(map[int32]map[int64]string)
	dmap[0] = make(map[int64]string)
	dmap[1] = make(map[int64]string)
	dmap[0][1] = "hello"
	dmap[0][2] = "world"
	dmap[1][1] = "test"
	dmap[1][2] = "only"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dummy(dmap[0][2])
	}
}

func BenchmarkMapTest02(b *testing.B) {
	dmap := make(map[int32]map[int64]string)
	dmap[0] = make(map[int64]string)
	dmap[1] = make(map[int64]string)
	dmap[0][1] = "hello"
	dmap[0][2] = "world"
	dmap[1][1] = "test"
	dmap[1][2] = "only"

	b.ResetTimer()
	a := dmap[0]
	for i := 0; i < b.N; i++ {
		dummy(a[2])
	}
}

func BenchmarkMapTest10(b *testing.B) {
	dmap := make(map[int32]map[int64]string)
	dmap[0] = make(map[int64]string)
	dmap[0][1] = "hello"
	dmap[0][2] = "world"
	dmap[0][3] = "test"
	dmap[0][4] = "only"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dummy(dmap[0][1])
		dummy(dmap[0][2])
		dummy(dmap[0][3])
		dummy(dmap[0][4])
	}
}

func BenchmarkMapTest11(b *testing.B) {
	dmap := make(map[int32]map[int64]string)
	dmap[0] = make(map[int64]string)
	dmap[0][1] = "hello"
	dmap[0][2] = "world"
	dmap[0][3] = "test"
	dmap[0][4] = "only"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a := dmap[0]
		dummy(a[1])
		dummy(a[2])
		dummy(a[3])
		dummy(a[4])
	}
}
