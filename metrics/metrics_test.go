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

package metrics

//import (
//	"testing"
//	"time"

//	"github.com/weibocom/wqs/config"
//)

//func TestAdd(t *testing.T) {
//	var testData = []struct {
//		k  string
//		el int64
//		lt int64
//	}{
//		{"queue#group#sent", 12, 0},
//		{"queue#group#sent", 1200, 0},
//		{"queue#group#sent", 120, 0},
//		{"queue#group#sent", 1, 0},
//		{"queue#group#recv", 59, 20},
//		{"queue#group#recv", 899, 2},
//	}
//	cfg, err := config.NewConfigFromFile("../config.properties")
//	if err != nil {
//		t.Fatal(err)
//	}
//	err = Start(cfg)
//	if err != nil {
//		t.Fatal(err)
//	}

//	for i := range testData {
//		if testData[i].lt <= 0 {
//			Add(testData[i].k, 1, testData[i].el)
//		} else {
//			Add(testData[i].k, 1, testData[i].el, testData[i].lt)
//		}
//	}
//	tm := time.NewTimer(time.Second * 2)
//	<-tm.C
//	tm.Stop()
//	client.stop()
//}
