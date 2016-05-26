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
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/weibocom/wqs/config"
	"github.com/weibocom/wqs/engine/queue"
	"github.com/weibocom/wqs/log"
	"github.com/weibocom/wqs/metrics"
	"github.com/weibocom/wqs/utils"
)

const (
	defaultGroup = "default"
)

type McServer struct {
	port         string
	queue        queue.Queue
	listener     *utils.Listener
	stopping     int32
	recvBuffSize int
	sendBuffSize int
	connPool     map[net.Conn]net.Conn
	mu           sync.Mutex
}

func NewMcServer(q queue.Queue, config *config.Config) *McServer {
	return &McServer{
		port:         config.McPort,
		queue:        q,
		recvBuffSize: config.McSocketRecvBuffer,
		sendBuffSize: config.McSocketSendBuffer,
		connPool:     make(map[net.Conn]net.Conn),
	}
}

func (ms *McServer) Start() error {

	var err error
	ms.listener, err = utils.Listen("tcp", fmt.Sprintf(":%s", ms.port))
	if err != nil {
		return errors.Trace(err)
	}
	log.Debugf("memcached protocol server start on %s port.", ms.port)
	go ms.mainLoop()
	return nil
}

func (ms *McServer) mainLoop() {

	for atomic.LoadInt32(&ms.stopping) == 0 {
		conn, err := ms.listener.Accept()
		if err != nil {
			log.Errorf("mc server accept error: %s", err)
			continue
		}
		if atomic.LoadInt32(&ms.stopping) != 0 {
			conn.Close()
			return
		}
		log.Debugf("mc server new client: %s", conn.RemoteAddr())
		ms.mu.Lock()
		ms.connPool[conn] = conn
		ms.mu.Unlock()
		go ms.connLoop(conn)
	}
}

func (ms *McServer) connLoop(conn net.Conn) {
	defer func(conn net.Conn) {
		log.Debugf("mc client closed :%s", conn.RemoteAddr())
		ms.mu.Lock()
		delete(ms.connPool, conn)
		ms.mu.Unlock()
		conn.Close()
		if err := recover(); err != nil {
			log.Errorf("mc connLoop panic error: %s", err)
		}
	}(conn)

	br := bufio.NewReaderSize(conn, ms.recvBuffSize)
	bw := bufio.NewWriterSize(conn, ms.sendBuffSize)

	for atomic.LoadInt32(&ms.stopping) == 0 {
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
		command, exists := commands[cmd]
		if !exists {
			command = commandUnkown
			cmd = "unsupported"
		}
		metrics.Add(cmd, 1)
		err = command(ms.queue, tokens, br, bw)
		bw.Flush()
		if err != nil {
			//command返回错误一定是不能容忍的错误，需要退出循环关闭连接，防止将后续有效数据的格式都破坏掉
			log.Errorf("mc bad command:%s", errors.ErrorStack(err))
			return
		}
	}
}

//close all connections of memcached protocol server.
func (ms *McServer) DrainConn() {
	ms.mu.Lock()
	for _, conn := range ms.connPool {
		conn.Close()
	}
	ms.mu.Unlock()
}

func (ms *McServer) Stop() {
	if !atomic.CompareAndSwapInt32(&ms.stopping, 0, 1) {
		return
	}
	//Make on processing commamd to run over
	time.Sleep(200 * time.Millisecond)
	if err := ms.listener.Close(); err != nil {
		log.Errorf("mc server listener close failed:%s", err)
		return
	}
	ms.DrainConn()
	for ms.listener.GetRemain() != 0 {
		time.Sleep(time.Millisecond)
	}
	log.Debugf("mc protocol server stop.")
}
