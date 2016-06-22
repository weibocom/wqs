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
	"net"
	"sync/atomic"

	"github.com/weibocom/wqs/metrics"

	"github.com/juju/errors"
)

type Listener struct {
	net.Listener
	count int64
}

type Conn struct {
	net.Conn
	closing int32
	parent  *Listener
}

func (c *Conn) Close() error {
	if atomic.CompareAndSwapInt32(&c.closing, 0, 1) {
		atomic.AddInt64(&(c.parent.count), -1)
		metrics.AddCounter(metrics.ReConn, -1)
		return c.Conn.Close()
	}
	return nil
}

func Listen(netType, laddr string) (*Listener, error) {

	l, err := net.Listen(netType, laddr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &Listener{Listener: l}, nil
}

func (l *Listener) Accept() (net.Conn, error) {

	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
		tcpConn.SetKeepAlive(true)
	}
	atomic.AddInt64(&l.count, 1)
	c := &Conn{Conn: conn, parent: l}
	metrics.AddCounter(metrics.ToConn, 1)
	metrics.AddCounter(metrics.ReConn, 1)
	//TODO 待讨论,是否需要提供链接池存储当前产生的c,然后重载Close(),主动关闭链接池中的链接。
	return c, nil
}

func (l *Listener) GetRemain() int64 {
	return atomic.LoadInt64(&l.count)
}
