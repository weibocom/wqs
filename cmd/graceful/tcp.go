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
	"sync"
	"time"

	"github.com/weibocom/wqs/log"
)

var (
	ErrWaitTimeout = errors.New("wait timeout")
)

const (
	_UNIX_SOCK = "/tmp/wqs_unix_sock_restart.sock"
)

type TCPServer struct {
	l   *net.TCPListener
	mgr *sync.WaitGroup
}

func NewTCPServer(port int) (*TCPServer, error) {
	s := &TCPServer{
		mgr: new(sync.WaitGroup),
	}
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("fail to resolve addr: %v", err)
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("fail to listen tcp: %v", err)
	}

	s.l = l
	return s, nil
}

func NewTCPServerFromFD(fd uintptr) (*TCPServer, error) {
	s := &TCPServer{
		mgr: new(sync.WaitGroup),
	}

	file := os.NewFile(fd, _UNIX_SOCK)
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
	for {
		conn, err := s.l.Accept()
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
				return
			}
		}
		go func() {
			s.mgr.Add(1)
			s.handleConn(conn)
			s.mgr.Done()
		}()
	}
}

func (s *TCPServer) handleConn(conn net.Conn) {
	log.Info("handle new conn")
	tick := time.NewTicker(time.Second)
	buffer := make([]byte, 64)
	for {
		select {
		case <-tick.C:
			_, err := conn.Write([]byte("helo"))
			if err != nil {
				conn.Close()
				return
			}

			_, err = conn.Read(buffer)
			if err != nil {
				conn.Close()
				return
			}
		}
	}
}
