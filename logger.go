package main

import (
	log "github.com/cihub/seelog"
	"github.com/juju/errors"
)

func initLogger(filename string) error {
	logger, err := log.LoggerFromConfigAsFile(filename)
	if err != nil {
		return errors.Trace(err)
	}
	log.ReplaceLogger(logger)
	return nil
}
