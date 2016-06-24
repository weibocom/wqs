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

package config

import (
	"math"
	"testing"
)

func TestSectionGetString(t *testing.T) {
	section := make(Section)
	section["A"] = "B"

	if a, err := section.GetString("A"); err == nil {
		if a != "B" {
			t.Fatalf("get error value : %q, want : %q", a, "B")
		}
	} else {
		t.Fatalf("unexpect err : %v", err)
	}

	if _, err := section.GetString("a"); err == nil {
		t.Fatalf("should occur not found error.")
	}

	if a := section.GetStringMust("a", "c"); a != "c" {
		t.Fatalf("get unexpect value : %q, want : %q", a, "c")
	}
}

func TestSectionGetBool(t *testing.T) {
	section := make(Section)
	section["A"] = "true"
	section["B"] = "True"
	section["C"] = "false"
	section["D"] = "False"

	if a, err := section.GetBool("A"); err == nil {
		if !a {
			t.Fatalf("get error value : %t, want : %t", a, true)
		}
	} else {
		t.Fatalf("unexpect err : %v", err)
	}

	if _, err := section.GetBool("a"); err == nil {
		t.Fatalf("should occur not found error.")
	}

	if a := section.GetBoolMust("a", true); !a {
		t.Fatalf("get unexpect value : %t, want : %t", a, true)
	}

	if a, err := section.GetBool("C"); err == nil {
		if a {
			t.Fatalf("get error value : %t, want : %t", a, false)
		}
	} else {
		t.Fatalf("unexpect err : %v", err)
	}
}

func TestSectionGetInt64(t *testing.T) {
	section := make(Section)
	section["A"] = "11"
	section["B"] = "01"
	section["C"] = "a"
	section["D"] = ""

	if a, err := section.GetInt64("A"); err == nil {
		if a != 11 {
			t.Fatalf("get error value : %d, want : %d", a, 11)
		}
	} else {
		t.Fatalf("unexpect err : %v", err)
	}

	if _, err := section.GetInt64("a"); err == nil {
		t.Fatalf("should occur not found error.")
	}

	if _, err := section.GetInt64("B"); err != nil {
		t.Fatalf("unexpect err : %v", err)
	}

	if _, err := section.GetInt64("C"); err == nil {
		t.Fatalf("should occur not found error.")
	}

	if _, err := section.GetInt64("D"); err == nil {
		t.Fatalf("should occur not found error.")
	}

	if a := section.GetInt64Must("a", 10); a != 10 {
		t.Fatalf("get unexpect value : %d, want : %d", a, 10)
	}
}

func TestSectionGetFloat64(t *testing.T) {
	section := make(Section)
	section["A"] = "100"
	section["B"] = "100.01"
	section["C"] = "a.b"
	section["D"] = "010.10"

	if a, err := section.GetFloat64("A"); err == nil {
		if math.Abs(a-100.0) > 1e-4 {
			t.Fatalf("get error value : %f, want : %f", a, 100.0)
		}
	} else {
		t.Fatalf("unexpect err : %v", err)
	}

	if _, err := section.GetFloat64("a"); err == nil {
		t.Fatalf("should occur not found error.")
	}

	if _, err := section.GetFloat64("B"); err != nil {
		t.Fatalf("unexpect err : %v", err)
	}

	if _, err := section.GetFloat64("C"); err == nil {
		t.Fatalf("should occur not found error.")
	}

	if _, err := section.GetFloat64("D"); err != nil {
		t.Fatalf("unexpect err : %v", err)
	}

	if a := section.GetFloat64Must("a", 9.9); math.Abs(a-9.9) > 1e-4 {
		t.Fatalf("get unexpect value : %f, want : %f", a, 9.9)
	}
}

func TestSectionGetDupByPattern(t *testing.T) {
	section := make(Section)
	section["A.B"] = "A.B"
	section["B.B"] = "B.B"
	section["A.C"] = "A.C"
	section["A.CB.B"] = "A.CB.B"
	section["AAbb"] = "AAbb"

	dup0 := section.GetDupByPattern(`\AA\.\w\z`)
	if len(dup0) != 2 {
		t.Fatalf("get empty duplication.")
	}
	if dup0["A.B"] != "A.B" || dup0["A.C"] != "A.C" {
		t.Fatalf("dup context wrong.")
	}

	dup1 := section.GetDupByPattern(`[0-9]+`)
	if len(dup1) != 0 {
		t.Fatalf("dup1 should be empty duplication.")
	}

	dup2 := section.GetDupByPattern(`^\w+\.B$`)
	if len(dup2) != 2 {
		t.Fatalf("get empty duplication.")
	}
	if dup2["A.B"] != "A.B" || dup2["B.B"] != "B.B" {
		t.Fatalf("dup context wrong.")
	}
}
