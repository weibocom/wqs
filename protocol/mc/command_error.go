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
	"io"

	"github.com/weibocom/wqs/service"

	"github.com/juju/errors"
)

var (
	ERROR                      = []byte("ERROR\r\n")
	CLIENT_ERROR_BADCMD_FORMAT = []byte("CLIENT_ERROR bad command line format\r\n")
	CLIENT_ERROR_BAD_DATACHUNK = []byte("CLIENT_ERROR bad data chunk\r\n")
	STORED                     = []byte("STORED\r\n")
	NOREPLY                    = []byte("noreply")
	ENGINE_ERROR_PREFIX        = []byte("SERVER_ERROR engine error ")
)

func commandUnkown(qservice service.QueueService, cmdLine []byte, lr LineReader, writer io.Writer) error {
	return errors.NotSupportedf("command %s ", string(cmdLine))
}
