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
	"bytes"
	"io"
	"strconv"
	"strings"

	log "github.com/cihub/seelog"
	"github.com/weibocom/wqs/service"
)

const (
	SET_NAME = "set"
)

func commandSet(qservice *service.QueueService, cmdLine []byte, lr LineReader, w io.Writer) (err error) {

	fields := Fields(cmdLine[4:]) // the first for bytes are "set "
	l := len(fields)
	if l < 4 || l > 5 {
		_, err = w.Write(ERROR)
		return
	}
	key := fields[0]
	flags := fields[1]
	exptime := fields[2]
	length := fields[3]
	log.Infof("mc set, key:%s, flags:%s, exptime:%s, length:%s", key, flags, exptime, length)
	var noreply []byte
	if l == 5 {
		noreply = fields[4]
	}
	data, err := lr.ReadLine()
	if err != nil && err != io.EOF {
		return err
	}
	dataLength, err := strconv.Atoi(string(length))
	if err != nil {
		_, err = w.Write(CLIENT_ERROR_BADCMD_FORMAT)
		return
	}
	if len(data) != dataLength {
		_, err = w.Write(CLIENT_ERROR_BAD_DATACHUNK)
		return
	}
	log.Debugf("mc command set, key:%s flags:%s exptime:%s len:%d, reply: %s, data: %s\n", key, flags, exptime, dataLength, noreply, data)
	keys := strings.Split(string(key), ".")
	queue := keys[0]
	var biz string
	if len(keys) > 1 {
		biz = keys[1]
	} else {
		biz = "default"
	}

	err = qservice.SendMsg(queue, biz, data)
	if err != nil {
		estr := err.Error()
		_, err = w.Write(ENGINE_ERROR_PREFIX)
		_, err = w.Write([]byte(estr))
		_, err = w.Write([]byte("\r\n"))
		return
	}

	if bytes.Equal(NOREPLY, noreply) {
		return
	}
	_, err = w.Write(STORED)
	return
}
