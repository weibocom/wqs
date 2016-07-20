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

package mc

import (
	"bufio"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/weibocom/wqs/engine/queue"
	"github.com/weibocom/wqs/log"
	"github.com/weibocom/wqs/utils"
)

type Server struct {
	addr         string
	queue        queue.Queue
	listener     *utils.Listener
	stopping     int32
	recvBuffSize int
	sendBuffSize int
	connPool     map[net.Conn]net.Conn
	mu           sync.Mutex
}

func NewServer(q queue.Queue, addr string, recvBuffSize, sendBuffSize int) *Server {
	return &Server{
		addr:         addr,
		queue:        q,
		recvBuffSize: recvBuffSize,
		sendBuffSize: sendBuffSize,
		connPool:     make(map[net.Conn]net.Conn),
	}
}

func (s *Server) Start() error {
	var err error
	s.listener, err = utils.Listen("tcp", s.addr)
	if err != nil {
		return errors.Trace(err)
	}

	log.Infof("memcached protocol server start on %s", s.addr)
	go s.mainLoop()
	return nil
}

func (s *Server) mainLoop() {
	for atomic.LoadInt32(&s.stopping) == 0 {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Errorf("mc server accept error: %s", err)
			continue
		}
		if atomic.LoadInt32(&s.stopping) != 0 {
			conn.Close()
			return
		}
		log.Debugf("mc server new client: %s", conn.RemoteAddr())
		s.mu.Lock()
		s.connPool[conn] = conn
		s.mu.Unlock()
		go s.connLoop(conn)
	}
}

func (s *Server) connLoop(conn net.Conn) {
	defer func(conn net.Conn) {
		log.Debugf("mc client closed :%s", conn.RemoteAddr())
		s.mu.Lock()
		delete(s.connPool, conn)
		s.mu.Unlock()
		conn.Close()
		if err := recover(); err != nil {
			log.Errorf("mc connLoop panic error: %s", err)
		}
	}(conn)

	br := bufio.NewReaderSize(conn, s.recvBuffSize)
	bw := bufio.NewWriterSize(conn, s.sendBuffSize)

	for atomic.LoadInt32(&s.stopping) == 0 {
		data, err := br.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Warnf("mc server ReadLine err:%s", err)
			return
		}

		tokens := strings.Split(strings.TrimSpace(data), " ")
		cmd := tokens[0]
		command, ok := commands[cmd]
		if !ok {
			command = commandUnkown
		}
		needClose := command(s.queue, tokens, br, bw)
		bw.Flush()
		if needClose {
			log.Errorf("memcached client %s ocurr error, close connection.", conn.RemoteAddr())
			return
		}
	}
}

//close all connections of memcached protocol server.
func (s *Server) DrainConn() {
	s.mu.Lock()
	for _, conn := range s.connPool {
		conn.Close()
	}
	s.mu.Unlock()
}

func (s *Server) Stop() {
	if !atomic.CompareAndSwapInt32(&s.stopping, 0, 1) {
		return
	}
	//Make on processing commamd to run over
	time.Sleep(200 * time.Millisecond)
	if err := s.listener.Close(); err != nil {
		log.Errorf("mc server listener close failed:%s", err)
		return
	}
	s.DrainConn()
	for s.listener.GetRemain() != 0 {
		time.Sleep(time.Millisecond)
	}
	log.Info("memcached protocol server stop.")
}
