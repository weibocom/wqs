package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/weibocom/wqs/cmd/graceful"
	"github.com/weibocom/wqs/log"
	"github.com/weibocom/wqs/utils"
)

const (
	envRestart = "WQS_RESTART"
)

// Avoid running with `go run main.go`
// You should make bin with `go build` firstly.
func main() {
	var s *graceful.TCPServer
	var err error
	if os.Getenv(envRestart) == "true" {
		s, err = graceful.NewTCPServerFromFD(3)
	} else {
		opts := []graceful.ServerOption{
			graceful.SetAddr(":12345"),
		}
		s, err = graceful.NewTCPServer(opts...)
	}
	if err != nil {
		log.Warnf("Init TCPServer err:%v", err)
		return
	}

	go s.AcceptLoop()

	utils.WritePid()
	signals := make(chan os.Signal)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGTERM)
	for sig := range signals {
		if sig == syscall.SIGTERM {
			s.Stop()
			err := s.WaitWithTimeout(10 * time.Second)
			if err != nil {
				os.Exit(-127)
			}
			os.Exit(0)
		} else if sig == syscall.SIGHUP {
			s.Stop()
			listenerFD, err := s.ListenerFD()
			if err != nil {
				log.Warnf("Get sockFD err:%v", err)
				return
			}
			os.Setenv(envRestart, "true")
			execSpec := &syscall.ProcAttr{
				Env:   os.Environ(),
				Files: []uintptr{os.Stdin.Fd(), os.Stdout.Fd(), os.Stderr.Fd(), listenerFD},
			}
			fork, err := syscall.ForkExec(os.Args[0], os.Args, execSpec)
			if err != nil {
				log.Warnf("Fork err:%v", err)
				return
			}
			utils.WritePidWithVal(fork)
			s.Wait()
			os.Exit(0)
		}
	}
}
