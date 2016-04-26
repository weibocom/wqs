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

package main

import (
	"log"
	"runtime"

	docopt "github.com/docopt/docopt-go"
	"github.com/juju/errors"
	"github.com/weibocom/wqs/utils"
)

var (
	globalMsgLength       = 500
	globalHost            = ""
	globalQueue           = ""
	globalBiz             = ""
	globalConcurrentLevel = 500
	globalDuration        = 60
)

var usage = `usage: benchmark [options] <command> [<args>...]

options:
	--cpu=<NUM>              set 
	--len=<LENGTH>           set message length (default 500)
	--host=<qservice_addr>   set qservice host IP:PORT
	--cc=<concurrent_level>  set concurrent goroutine number
	--time=<time>			set benchmark duration(default 60 second)
	--queue=<queue>          set test queue name
	--biz=<biz>              set test biz name

commands:
	mc
	http
`

func fatal(msg interface{}) {

	switch msg.(type) {
	case string:
		log.Print(msg)
	case error:
		log.Print(errors.ErrorStack(msg.(error)))
	}
}

func main() {

	var err error
	args, err := docopt.Parse(usage, nil, true, "qservice benchmark v0.1", true)
	if err != nil {
		fatal(err)
	}

	ncpu, err := utils.GetIntFromArgs(args, "--cpu", runtime.NumCPU())
	if err != nil {
		fatal(err)
	}

	if v := args["--host"]; v != nil {
		globalHost = v.(string)
	}

	if v := args["--queue"]; v != nil {
		globalQueue = v.(string)
	}

	if v := args["--biz"]; v != nil {
		globalBiz = v.(string)
	}

	globalMsgLength, err = utils.GetIntFromArgs(args, "--len", globalMsgLength)
	if err != nil {
		fatal(err)
	}

	globalConcurrentLevel, err = utils.GetIntFromArgs(args, "--cc", globalConcurrentLevel)
	if err != nil {
		fatal(err)
	}

	globalDuration, err = utils.GetIntFromArgs(args, "--time", globalDuration)
	if err != nil {
		fatal(err)
	}

	runtime.GOMAXPROCS(ncpu)

	cmd := args["<command>"].(string)
	cmdArgs := args["<args>"].([]string)

	err = runCommand(cmd, cmdArgs)
	if err != nil {
		fatal(err)
	}
}

func runCommand(cmd string, args []string) (err error) {
	argv := make([]string, 1)
	argv[0] = cmd
	argv = append(argv, args...)
	switch cmd {
	case "mc":
		return errors.Trace(cmdBenchmarkMC(argv))
	case "http":
		return errors.Trace(cmdBenchmarkHttp(argv))
	}
	return errors.NotSupportedf("See 'benchmark -h', command %s", cmd)
}
