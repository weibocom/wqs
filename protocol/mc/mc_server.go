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
	"bytes"
	"fmt"
	"io"
	"net"

	log "github.com/cihub/seelog"
	"github.com/juju/errors"
	"github.com/weibocom/wqs/config"
	"github.com/weibocom/wqs/service"
)

type McServer struct {
	port         string
	queueService *service.QueueService
	listener     net.Listener
	recvBuffSize int
	sendBuffSize int
}

func NewMcServer(queueService *service.QueueService, config *config.Config) *McServer {
	return &McServer{
		port:         config.McPort,
		queueService: queueService,
		recvBuffSize: config.McSocketRecvBuffer,
		sendBuffSize: config.McSocketSendBuffer,
	}
}

func (this *McServer) Start() error {

	var err error
	this.listener, err = net.Listen("tcp", fmt.Sprintf(":%s", this.port))
	if err != nil {
		return errors.Trace(err)
	}

	log.Infof("proxy McServer started and listen on %s", this.port)
	defer this.Close()

	for {
		conn, err := this.listener.Accept()
		if err != nil {
			log.Errorf("Accept error: %s", err.Error())
		}
		conn.(*net.TCPConn).SetNoDelay(true)
		conn.(*net.TCPConn).SetKeepAlive(true)
		// stats.IncrConn()
		log.Infof("Accepted client: %s", conn.RemoteAddr())
		go this.connLoop(conn)
	}
}

func (this *McServer) connLoop(conn net.Conn) {
	defer func() {
		if err := recover(); err != nil {
			log.Errorf("panic error: %s", err)
		}
		log.Infof("connection colsed. remote:%s", conn.RemoteAddr())
		conn.Close() // ignore the close error
		// stats.DecrConn()
	}()

	var finalErr error
	br := NewBufferedLineReader(conn, this.recvBuffSize)
	bw := bufio.NewWriterSize(conn, this.sendBuffSize)

MainLoop:
	for {
		line, err := br.ReadLine()
		if err != nil {
			if err == io.EOF {
				break MainLoop
			}
			finalErr = errors.Trace(err)
			break MainLoop
		}
		cmdIdx := bytes.IndexByte(line, ' ')
		cName := ""
		if cmdIdx > 0 {
			cName = string(line[0:cmdIdx])
		} else {
			cName = string(line) // total line as a command
		}
		command, exists := commands[cName]
		if !exists {
			command = commandUnkown
		}
		err = command(*this.queueService, line, br, bw)
		br.Reset()
		bw.Flush()
		if err != nil {
			finalErr = errors.Trace(err)
		}
	}

	if finalErr != nil {
		log.Warnf("connection loop error:%s", errors.ErrorStack(finalErr))
	}
}

func (this *McServer) Close() {
	log.Warnf("proxy server will be close immediately on %s", this.listener.Addr().String())
	log.Infof("proxy server stop listen on %s", this.listener.Addr().String())
	if err := this.listener.Close(); err != nil {
		log.Warnf("proxy server listener close failed:%s", this.listener.Addr().String())
	}

	// cnum := stats.GetConn()
	// if cnum > 0 {
	// 	// log.Infof("%d connections alive, waiting to be closed", cnum)
	// 	timeout := time.Now().Add(3 * time.Second) // waiting 3 seconds for closing all connections

	// 	for {
	// 		select {
	// 		case <-time.After(100 * time.Millisecond):
	// 			cnum = stats.GetConn()
	// 			if cnum == 0 {
	// 				break
	// 			}
	// 			if time.Now().After(timeout) { // timeout
	// 				break
	// 			}
	// 		}
	// 	}
	// }

	// cnum = stats.GetConn()
	// if cnum > 0 {
	// 	log.Warnf("%d connections not closed normally", cnum)
	// }
}
