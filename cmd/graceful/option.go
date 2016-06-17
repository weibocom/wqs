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
	"net"
)

type Option struct {
	Addr     *net.TCPAddr
	SockName string
	Err      error
}

func SetAddr(addrStr string) ServerOption {
	return func(o *Option) {
		addr, err := net.ResolveTCPAddr("tcp", addrStr)
		if err != nil {
			o.Err = err
			return
		}
		o.Addr = addr
	}
}

func SetUnixSock(sock string) ServerOption {
	return func(o *Option) {
		o.SockName = sock
	}
}
