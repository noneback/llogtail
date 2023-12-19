package llogtail

import (
	"os"
	"testing"
)

func TestCollector(t *testing.T) {
	Pre()
	dir, pattern := "tests", "*.log"
	fpath := "tests/info.log"
	info, err := os.Stat(fpath)
	if err != nil {
		logger.Errorf("os.Stat %v -> %v", fpath, err)
		t.FailNow()
	}

	meta, err := genLogMeta(info, path, dir, pattern)
	if err != nil {
		logger.Errorf("mock log meta -> %v", err)
		t.FailNow()
	}

	t.Run("Test Collector Init", func(t *testing.T) {
		c := newCollector(meta)
	})

}
