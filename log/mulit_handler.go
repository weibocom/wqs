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
	"sync"
)

var profRWLock sync.RWMutex
var profLogger *Logger

var opsRWLock sync.RWMutex
var opsLogger [logLevelMax]*Logger

func init() {
	pLogger := &Logger{fd: os.Stdout, level: LogInfo, flags: LstdFlags | Llevel}
	RestProfileLogger(pLogger)
	for i := LogFatal; i < logLevelMax; i++ {
		RestLogger(pLogger, i)
	}
}

func RestProfileLogger(logger *Logger) {
	profRWLock.Lock()
	logger.SetFlags(LstdFlags)
	logger.SetLogLevel(LogInfo)
	profLogger = logger
	profRWLock.Unlock()
}

func ProfileSetFlags(flags uint32) {
	profRWLock.RLock()
	profLogger.SetFlags(flags)
	profRWLock.RUnlock()
}

func Profile(format string, args ...interface{}) {
	profRWLock.RLock()
	profLogger.Output(3, LogInfo, fmt.Sprintf(format, args...))
	profRWLock.RUnlock()
}

func RestLogger(logger *Logger, levels ...uint32) {
	opsRWLock.Lock()
	for _, level := range levels {
		opsLogger[level] = logger
	}
	opsRWLock.Unlock()
}

func GetLogger(level uint32) *Logger {
	opsRWLock.RLock()
	defer opsRWLock.RUnlock()
	return opsLogger[level]
}

func Fatal(args ...interface{}) {
	opsRWLock.RLock()
	opsLogger[LogFatal].log(LogFatal, args...)
	opsRWLock.RUnlock()
	os.Exit(-1)
}

func Fatalf(format string, args ...interface{}) {
	opsRWLock.RLock()
	opsLogger[LogFatal].logf(LogFatal, format, args...)
	opsRWLock.RUnlock()
	os.Exit(-1)
}

func Error(args ...interface{}) {
	opsRWLock.RLock()
	opsLogger[LogError].log(LogError, args...)
	opsRWLock.RUnlock()
}

func Errorf(format string, args ...interface{}) {
	opsRWLock.RLock()
	opsLogger[LogError].logf(LogError, format, args...)
	opsRWLock.RUnlock()
}

func Debug(args ...interface{}) {
	opsRWLock.RLock()
	opsLogger[LogDebug].log(LogDebug, args...)
	opsRWLock.RUnlock()
}

func Debugf(format string, args ...interface{}) {
	opsRWLock.RLock()
	opsLogger[LogDebug].logf(LogDebug, format, args...)
	opsRWLock.RUnlock()
}

func Warn(args ...interface{}) {
	opsRWLock.RLock()
	opsLogger[LogWarning].log(LogWarning, args...)
	opsRWLock.RUnlock()
}

func Warnf(format string, args ...interface{}) {
	opsRWLock.RLock()
	opsLogger[LogWarning].logf(LogWarning, format, args...)
	opsRWLock.RUnlock()
}

func Info(args ...interface{}) {
	opsRWLock.RLock()
	opsLogger[LogInfo].log(LogInfo, args...)
	opsRWLock.RUnlock()
}

func Infof(format string, args ...interface{}) {
	opsRWLock.RLock()
	opsLogger[LogInfo].logf(LogInfo, format, args...)
	opsRWLock.RUnlock()
}
