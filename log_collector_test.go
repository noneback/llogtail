package llogtail

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestLogCollector(t *testing.T) {
	Pre()
	InitLogger(&defauleLogOption)
	collector := NewLogCollector()
	conf, err := ReadLogCollectorConf("./tests/collector.json")
	if err != nil {
		logger.Errorf("read conf -> %w", err)
		t.FailNow()
	}

	t.Run("Test Collector Init", func(t *testing.T) {
		if err := collector.Init(*conf); err != nil {
			logger.Error("Init -> %w", err)
			t.FailNow()
		}
	})
	defer collector.Close()

	t.Run("Test Collector Collect Once", func(t *testing.T) {
		collector.Run()
		expected := make(map[string][][]byte) // path -> data
		// mock data
		for i := 0; i < 10; i++ {
			mock := generateDataOneKB()
			path := kLogs[i%3]
			expected[path] = append(expected[path], mock)
			modify(path, kDataOneKB)
			time.Sleep(time.Millisecond * 50)
		}

		time.Sleep(200 * time.Millisecond)
		sconf, err := readFileSinkConf("./tests/sink.json")
		if err != nil {
			logger.Errorf("read sink conf -> %w", err)
			t.FailNow()
		}

		if err := kCheckContent(expected, sconf.Dst); err != nil {
			logger.Errorf("check content -> %v", err.Error())
			t.FailNow()
		}
	})
}

func kCheckContent(expected map[string][][]byte, target string) error {
	fd, err := os.Open(target)
	if err != nil {
		return fmt.Errorf("open %v -> %w", target, err)
	}
	defer fd.Close()
	cursors := make(map[string]int)
	for k := range cursors {
		cursors[k] = 0
	}

	scanner := bufio.NewScanner(fd)
	unit := kDataUnit{}
	for scanner.Scan() {
		line := scanner.Text()
		if err := json.Unmarshal(UnsafeStringToSlice(line), &unit); err != nil {
			return fmt.Errorf("unmarshal line to uint -> %w", err)
		}
		cursor := cursors[unit.File]
		cursors[unit.File] = cursor + 1

		expected_content := expected[unit.File][cursor]

		if !bytes.Equal(expected_content, unit.Content) {
			logger.Warningf("except %v, got %v", string(expected_content), string(unit.Content))
			return fmt.Errorf("Content Not Equal %v at %v", unit.File, cursor)
		}
	}
	return nil
}
