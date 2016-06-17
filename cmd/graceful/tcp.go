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
package graceful

import (
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/weibocom/wqs/log"
)

var (
	ErrWaitTimeout = errors.New("wait timeout")

	// Be happy for test
	defaultHandle   = mockHandleConn
	defaultUnixSock = "/tmp/wqs_unix_sock_restart.sock"
)

type ServerOption func(opt *Option)

type TCPServer struct {
	l   *net.TCPListener
	mgr *ConnectionManager
	opt *Option

	handle func(net.Conn) error
}

func NewTCPServer(opts ...ServerOption) (*TCPServer, error) {
	s := &TCPServer{
		opt: new(Option),
		mgr: newConnectionMgr(),
	}
	for i := range opts {
		opts[i](s.opt)
	}
	if s.opt.Err != nil {
		return nil, s.opt.Err
	}
	l, err := net.ListenTCP("tcp", s.opt.Addr)
	if err != nil {
		return nil, fmt.Errorf("fail to listen tcp: %v", err)
	}

	s.l = l
	return s, nil
}

func NewTCPServerFromFD(fd uintptr) (*TCPServer, error) {
	s := &TCPServer{
		mgr: newConnectionMgr(),
	}

	file := os.NewFile(fd, defaultUnixSock)
	listener, err := net.FileListener(file)
	if err != nil {
		return nil, errors.New("File to recover socket from file descriptor: " + err.Error())
	}
	listenerTCP, ok := listener.(*net.TCPListener)
	if !ok {
		return nil, fmt.Errorf("File descriptor %d is not a valid TCP socket", fd)
	}
	s.l = listenerTCP
	return s, nil
}

func (s *TCPServer) Stop() {
	s.l.SetDeadline(time.Now())
}

func (s *TCPServer) ListenerFD() (uintptr, error) {
	file, err := s.l.File()
	if err != nil {
		return 0, err
	}
	return file.Fd(), nil
}

func (s *TCPServer) Wait() {
	// TODO
	s.mgr.Wait()
}

func (s *TCPServer) WaitWithTimeout(duration time.Duration) error {
	timeout := time.NewTimer(duration)
	wait := make(chan struct{})
	go func() {
		s.Wait()
		wait <- struct{}{}
	}()

	select {
	case <-timeout.C:
		return ErrWaitTimeout
	case <-wait:
		return nil
	}
}

func (s *TCPServer) AcceptLoop() {
	if s.handle == nil {
		s.handle = defaultHandle
	}

	for {
		conn, err := s.l.Accept()
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
				return
			}
		}
		go func() {
			s.mgr.Add(1)
			s.handle(conn)
			s.mgr.Done()
		}()
	}
}

func mockHandleConn(conn net.Conn) error {
	log.Info("handle new conn")
	tick := time.NewTicker(time.Second)
	buffer := make([]byte, 64)
	for {
		select {
		case <-tick.C:
			_, err := conn.Write([]byte("helo"))
			if err != nil {
				conn.Close()
				return err
			}

			_, err = conn.Read(buffer)
			if err != nil {
				conn.Close()
				return err
			}
		}
	}
}
