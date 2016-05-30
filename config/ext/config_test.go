package ext

import (
	"testing"
)

func TestLoad(t *testing.T) {
	c, err := NewConfig("test.conf", 0)
	if err != nil {
		t.FailNow()
	}
	t.Log(c)
}

func TestGetSection(t *testing.T) {
	c, err := NewConfig("test.conf", 0)
	if err != nil {
		t.FailNow()
	}
	sec, err := c.GetSection("metrics")
	if err != nil {
		t.FailNow()
	}
	t.Logf("sec = %v", sec)
	val, err := sec.GetString("center")
	if err != nil {
		println(err.Error())
		t.FailNow()
	}
	println(val)
	print(string(c.DumpConf()))
}
