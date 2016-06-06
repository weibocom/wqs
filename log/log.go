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
	"path"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type RollingType uint32

const (
	// Bits or'ed together to control what's printed.
	// There is no control over the order they appear (the order listed
	// here) or the format they present (as described in the comments).
	// The prefix is followed by a colon only when Llongfile or Lshortfile
	// is specified.
	// For example, flags Ldate | Ltime (or LstdFlags) produce,
	//	2009/01/23 01:23:23 message
	// while flags Ldate | Ltime | Lmicroseconds | Llongfile produce,
	//	2009/01/23 01:23:23.123123 /a/b/c/d.go:23: message
	Ldate      = 1 << iota     // the date in the local time zone: 2009/01/23
	Ltime                      // the time in the local time zone: 01:23:23
	Llongfile                  // full file name and line number: /a/b/c/d.go:23
	Lshortfile                 // final file name element and line number: d.go:23. overrides Llongfile
	Llevel                     //
	LUTC                       // if Ldate or Ltime is set, use UTC rather than the local time zone
	LstdFlags  = Ldate | Ltime // initial values for the standard logger
)

const (
	LogFatal uint32 = iota
	LogError
	LogWarning
	LogInfo
	LogDebug
	logLevelMax
)

const (
	LogFatalS   = "fatal"
	LogErrorS   = "error"
	LogWarningS = "warning"
	LogInfoS    = "info"
	LogDebugS   = "debug"
)

const (
	RollingDeny RollingType = iota
	RollingByHour
	RollingByDay
)

type Logger struct {
	level       uint32
	rolling     RollingType
	fileName    string
	fileSuffix  string
	flags       uint32
	shouldClose bool
	fd          *os.File
	mu          sync.Mutex
	suffixTime  time.Time
	toRoll      time.Time     //Next time of to cut file.
	duration    time.Duration //The duration to cut file.
	buf         []byte
}

func NewLogger(file string) *Logger {
	return &Logger{
		fileName: file,
		level:    LogInfo,
		flags:    LstdFlags | Llevel,
	}
}

func (l *Logger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.shouldClose {
		return nil
	}
	return l.fd.Close()
}

func (l *Logger) Open() (*Logger, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	err := os.MkdirAll(path.Dir(l.fileName), 0755)
	if err != nil && !os.IsExist(err) {
		return l, err
	}
	l.shouldClose = true
	l.fd, err = os.OpenFile(l.fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	return l, err
}

func (l *Logger) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.fd.Sync()
}

//Set Logger output level.
func (l *Logger) SetLogLevel(level uint32) {
	atomic.StoreUint32(&l.level, level)
}

func (l *Logger) SetFlags(flags uint32) {
	atomic.StoreUint32(&l.flags, flags)
}

func genNextClock() (time.Time, time.Time) {
	now := time.Now().Local()
	year, month, day := now.Date()
	hour, _, _ := now.Clock()
	return time.Date(year, month, day, hour, 0, 0, 0, time.Local),
		time.Date(year, month, day, hour+1, 0, 0, 0, time.Local)
}

func genNextDay() (time.Time, time.Time) {
	year, month, day := time.Now().Local().Date()
	return time.Date(year, month, day, 0, 0, 0, 0, time.Local),
		time.Date(year, month, day+1, 0, 0, 0, 0, time.Local)
}

func (l *Logger) GetLevel() uint32 {
	return atomic.LoadUint32(&l.level)
}

func (l *Logger) SetRolling(t RollingType) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.rolling = t
	switch t {
	case RollingByHour:
		l.duration = time.Hour
		l.fileSuffix = "20060102-15"
		l.suffixTime, l.toRoll = genNextClock()
	case RollingByDay:
		l.duration = time.Hour * 24
		l.fileSuffix = "20060102"
		l.suffixTime, l.toRoll = genNextDay()
	}
}

func (l *Logger) rotate(t time.Time) {
	if l.rolling == RollingByDay || l.rolling == RollingByHour {
		if t.After(l.toRoll) {
			var err error
			suffix := l.suffixTime.Format(l.fileSuffix)
			fileName := fmt.Sprintf("%s.%s", l.fileName, suffix)
			if err = l.fd.Close(); err != nil {
				fmt.Fprintf(os.Stderr, "Logger Close %s error: %s\n", l.fileName, err)
				os.Exit(-1)
			}
			if err = os.Rename(l.fileName, fileName); err != nil {
				fmt.Fprintf(os.Stderr, "Logger Rename %s error: %s\n", fileName, err)
				os.Exit(-1)
			}
			l.fd, err = os.OpenFile(l.fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Logger Open %s error: %s\n", l.fileName, err)
				os.Exit(-1)
			}
			l.toRoll = l.toRoll.Add(l.duration)
			l.suffixTime = l.suffixTime.Add(l.duration)
		}
	}
}

func itoa(buf *[]byte, i int, wid int) {
	// Assemble decimal in reverse order.
	var b [20]byte
	bp := len(b) - 1
	for i >= 10 || wid > 1 {
		wid--
		q := i / 10
		b[bp] = byte('0' + i - q*10)
		bp--
		i = q
	}
	// i < 10
	b[bp] = byte('0' + i)
	*buf = append(*buf, b[bp:]...)
}

func logLevel2String(t uint32) string {
	switch t {
	case LogFatal:
		return "[FATAL]"
	case LogError:
		return "[ERROR]"
	case LogDebug:
		return "[DEBUG]"
	case LogWarning:
		return "[WARNING]"
	case LogInfo:
		return "[INFO]"
	}
	return "[UNKOWN]"
}

func LogLevel2String(level uint32) string {
	switch level {
	case LogFatal:
		return "fatal"
	case LogError:
		return "error"
	case LogDebug:
		return "debug"
	case LogWarning:
		return "warning"
	case LogInfo:
		return "info"
	}
	return "unknow"
}

func (l *Logger) formatPrefix(buf *[]byte, flags uint32, t time.Time, file string, line int, level uint32) {
	if flags&LUTC != 0 {
		t = t.UTC()
	}
	if flags&Ldate != 0 {
		year, month, day := t.Date()
		itoa(buf, year, 4)
		*buf = append(*buf, '-')
		itoa(buf, int(month), 2)
		*buf = append(*buf, '-')
		itoa(buf, day, 2)
		*buf = append(*buf, ' ')
	}
	if flags&Ltime != 0 {
		hour, min, sec := t.Clock()
		itoa(buf, hour, 2)
		*buf = append(*buf, ':')
		itoa(buf, min, 2)
		*buf = append(*buf, ':')
		itoa(buf, sec, 2)
		*buf = append(*buf, ' ')
	}
	if flags&Llevel != 0 {
		lstr := logLevel2String(level)
		*buf = append(*buf, lstr...)
		*buf = append(*buf, ' ')
	}
	if flags&(Lshortfile|Llongfile) != 0 {
		if flags&Lshortfile != 0 {
			short := file
			for i := len(file) - 1; i > 0; i-- {
				if file[i] == '/' {
					short = file[i+1:]
					break
				}
			}
			file = short
		}
		*buf = append(*buf, file...)
		*buf = append(*buf, ':')
		itoa(buf, line, -1)
		*buf = append(*buf, ": "...)
	}
}

//Low level write handler. Usually you should not use this API.
func (l *Logger) Output(calldepth int, level uint32, s string) error {
	var file string
	var line int
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rotate(now)
	flags := atomic.LoadUint32(&l.flags)
	if flags&(Lshortfile|Llongfile) != 0 {
		// release lock while getting caller info - it's expensive.
		l.mu.Unlock()
		var ok bool
		_, file, line, ok = runtime.Caller(calldepth)
		if !ok {
			file = "???"
			line = 0
		}
		l.mu.Lock()
	}
	l.buf = l.buf[:0]
	l.formatPrefix(&l.buf, flags, now, file, line, level)
	l.buf = append(l.buf, s...)
	if len(s) == 0 || s[len(s)-1] != '\n' {
		l.buf = append(l.buf, '\n')
	}
	_, err := l.fd.Write(l.buf)
	if level == LogFatal {
		fmt.Fprint(os.Stderr, string(l.buf))
	}
	return err
}

func (l *Logger) log(level uint32, args ...interface{}) {
	if level > atomic.LoadUint32(&l.level) {
		return
	}
	l.Output(3, level, fmt.Sprint(args...))
}

func (l *Logger) logformat(level uint32, format string, args ...interface{}) {
	if level > atomic.LoadUint32(&l.level) {
		return
	}
	l.Output(3, level, fmt.Sprintf(format, args...))
}

func (l *Logger) Fatal(args ...interface{}) {
	l.log(LogFatal, args...)
	l.Close()
	os.Exit(-1)
}

func (l *Logger) Fatalf(format string, args ...interface{}) {
	l.logformat(LogFatal, format, args...)
	l.Close()
	os.Exit(-1)
}

func (l *Logger) Error(args ...interface{}) {
	l.log(LogError, args...)
}

func (l *Logger) Errorf(format string, args ...interface{}) {
	l.logformat(LogError, format, args...)
}

func (l *Logger) Warn(args ...interface{}) {
	l.log(LogWarning, args...)
}

func (l *Logger) Warnf(format string, args ...interface{}) {
	l.logformat(LogWarning, format, args...)
}

func (l *Logger) Debug(args ...interface{}) {
	l.log(LogDebug, args...)
}

func (l *Logger) Debugf(format string, args ...interface{}) {
	l.logformat(LogDebug, format, args...)
}

func (l *Logger) Info(args ...interface{}) {
	l.log(LogInfo, args...)
}

func (l *Logger) Infof(format string, args ...interface{}) {
	l.logformat(LogInfo, format, args...)
}
