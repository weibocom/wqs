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
	"github.com/weibocom/wqs/utils"
)

type McServer struct {
	port         string
	queueService *service.QueueService
	listener     *utils.Listener
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

func (ms *McServer) Start() error {

	var err error
	ms.listener, err = utils.Listen("tcp", fmt.Sprintf(":%s", ms.port))
	if err != nil {
		return errors.Trace(err)
	}

	log.Infof("proxy McServer started and listen on %s", ms.port)
	for {
		conn, err := ms.listener.Accept()
		if err != nil {
			log.Errorf("Accept error: %s", err.Error())
		}
		log.Debugf("Accepted client: %s", conn.RemoteAddr())
		go ms.connLoop(conn)
	}
}

func (ms *McServer) connLoop(conn net.Conn) {
	defer func() {
		if err := recover(); err != nil {
			log.Errorf("panic error: %s", err)
		}
		log.Debugf("connection colsed. remote:%s", conn.RemoteAddr())
		conn.Close()
	}()

	br := NewBufferedLineReader(conn, ms.recvBuffSize)
	bw := bufio.NewWriterSize(conn, ms.sendBuffSize)

	for {
		line, err := br.ReadLine()
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Debugf("mc readline err:%s", err)
			return
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
		err = command(ms.queueService, line, br, bw)
		br.Reset()
		bw.Flush()
		if err != nil {
			log.Debugf("mc command err:%s", errors.ErrorStack(err))
		}
	}
}

func (ms *McServer) Close() {

	log.Infof("McServer Close() be called.")
	if err := ms.listener.Close(); err != nil {
		log.Errorf("McServer listener close failed:%s", err)
	}
}

func (ms *McServer) Closed() bool {
	return ms.listener.GetRemain() == 0
}
