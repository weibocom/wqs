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
	"os"

	docopt "github.com/docopt/docopt-go"
	"github.com/juju/errors"
)

var usage = `usage: states [options] <command> [<args>...]

options:
	--zookeeper                zookeeper address

commands:
	kafka_group                read kafka group's state
`

var (
	zookeeper string
)

func fatal(msg interface{}) {

	switch msg.(type) {
	case string:
		log.Print(msg)
	case error:
		log.Print(errors.ErrorStack(msg.(error)))
	}
	os.Exit(-1)
}

func main() {

	args, err := docopt.Parse(usage, nil, true, "qservice benchmark v0.1", true)
	if err != nil {
		fatal(err)
	}

	//	if zookeeper = args["--zookeeper"]; len(zookeeper) == 0 {
	//		fatal("invalied zookeeper address")
	//	}

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
	case "kafka_group":
		return errors.Trace(cmdKafkaGroup(argv))
	}
	return errors.NotSupportedf("See 'states -h', command %s", cmd)
}
