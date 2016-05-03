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

package log

import (
	"fmt"
	"os"
	"testing"
)

func doPerLevel(l *Logger) {
	num := 1000
	str := "abc"
	l.Error(num, " ", str)
	l.Errorf("a %d %s", 10000, "abc")
	l.Debug(num, " ", str)
	l.Debugf("a %d %s", 10000, "abc")
	l.Info(num, " ", str)
	l.Infof("a %d %s", 10000, "abc")
}

var stdLogger = &Logger{fd: os.Stdout}

func TestFlags(t *testing.T) {
	t.Parallel()
	stdLogger.SetFlags(Ldate | Llevel | Ltime | Lshortfile)
	for i := LogFatal; i < logLevelMax; i++ {
		level := logLevel2String(i)
		fmt.Println(level, ": ")
		stdLogger.SetLogLevel(i)
		doPerLevel(stdLogger)
	}
}

func TestFlagsForRace(t *testing.T) {
	t.Parallel()
	stdLogger.SetFlags(Ldate | Llevel | Ltime)
	for i := LogFatal; i < logLevelMax; i++ {
		level := logLevel2String(i)
		fmt.Println(level, ": ")
		stdLogger.SetLogLevel(i)
		doPerLevel(stdLogger)
	}
}

func TestProfile(t *testing.T) {
	Profile("q: %d t:%s", 1000, "20160501")
}

func TestGetLogger(t *testing.T) {
	for i := LogFatal; i < logLevelMax; i++ {
		want := stdLogger
		RestLogger(want, LogError)
		get := GetLogger(LogError)
		if get != want {
			t.Errorf("GetLogger error: want %v, get %v", want, get)
		}
	}
}

func TestLogger(t *testing.T) {
	logger := &Logger{fd: os.Stderr}
	logger.SetLogLevel(LogError)
	logger.SetFlags(LstdFlags | Lshortfile | Llevel)
	RestLogger(logger, LogError)
	Errorf("eeeeeeee")
}
