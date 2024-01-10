package llogtail

import (
	"fmt"
	"os"
	"testing"
)

func mockLogMeta() (*LogMeta, error) {
	dir, pattern := "tests", "*.log"
	fpath := "tests/info.log"
	info, err := os.Stat(fpath)
	if err != nil {
		return nil, fmt.Errorf("os.Stat %v -> %v", fpath, err)
	}

	meta, err := genLogMeta(info, fpath, dir, pattern)
	if err != nil {
		return nil, fmt.Errorf("mock log meta -> %v", err)
	}
	return meta, nil
}

func TestCollector(t *testing.T) {
	Pre()
	meta, err := mockLogMeta()
	if err != nil {
		logger.Errorf("mock log meta -> %v", err)
		t.FailNow()
	}
	c := newCollector(meta)
	t.Run("Test Collector Init", func(t *testing.T) {
		if err := c.init(); err != nil {
			logger.Errorf("init -> %v", err)
			t.FailNow()
		}
	})

	t.Run("Test Collector Collect", func(t *testing.T) {
		raw, err := c.fetch()
		if err != nil {
			logger.Errorf("fetct -> %v", err.Error())
			t.FailNow()
		}
		logger.Info(raw)
	})

}
