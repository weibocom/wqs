package main

import (
	"flag"
	"strings"

	"github.com/weibocom/wqs/metrics/term"
)

func main() {
	var (
		port   int
		page   int
		height int
		width  int
		hosts  string
	)
	flag.IntVar(&port, "p", 10001, "bind port")
	flag.IntVar(&page, "g", 10, "per page")
	flag.IntVar(&height, "h", 6, "height")
	flag.IntVar(&width, "w", 60, "width")
	flag.StringVar(&hosts, "e", "", "endpoints, sep by #,such as bj.01#bj.02")
	flag.Parse()

	hs := strings.Split(hosts, "#")
	if hosts == "" {
		hs = []string{}
	}

	s := term.Init(port, page, width, height, hs)
	s.Start()
}
