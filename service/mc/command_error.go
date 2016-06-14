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

	"github.com/weibocom/wqs/engine/queue"

	"github.com/juju/errors"
)

const (
	noReply                     = "noreply"
	respValue                   = "VALUE"
	respEnd                     = "END\r\n"
	respError                   = "ERROR\r\n"
	respStored                  = "STORED\r\n"
	respClientErrorBadDatachunk = "CLIENT_ERROR bad data chunk\r\n"
	respClientErrorBadCmdFormat = "CLIENT_ERROR bad command line format\r\n"
	respEngineErrorPrefix       = "SERVER_ERROR engine error"
)

func init() {
	registerCommand(cmdUnknown, commandUnkown)
}

func commandUnkown(q queue.Queue, tokens []string, r *bufio.Reader, w *bufio.Writer) error {
	return errors.NotSupportedf("command %s ", tokens[0])
}
