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

	t.Run("Test Collector Collect", func(t *testing.T) {
		collector.Run()
		expected := make(map[string][]byte) // path -> data
		// mock data
		for i := 0; i < 100; i++ {
			mock := generateDataOneKB()
			path := kLogs[i%3]
			seq := expected[path]

			logger.Debugf("mock -> %v to %v", string(mock[:10]), path)
			seq = append(seq, mock...)
			expected[path] = seq
			appendf(path, mock)
			time.Sleep(time.Millisecond * 50)
		}

		time.Sleep(1000 * time.Millisecond)
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

func kCheckContent(expected map[string][]byte, target string) error {
	fd, err := os.Open(target)
	if err != nil {
		return fmt.Errorf("open %v -> %w", target, err)
	}
	defer fd.Close()
	got := make(map[string][]byte)
	for k := range expected {
		got[k] = make([]byte, 0, 1024)
	}

	scanner := bufio.NewScanner(fd)
	unit := kDataUnit{}
	for scanner.Scan() {
		line := scanner.Text()
		if err := json.Unmarshal(UnsafeStringToSlice(line), &unit); err != nil {
			return fmt.Errorf("unmarshal line to uint -> %w", err)
		}
		got[unit.File] = append(got[unit.File], unit.Content...)
	}

	for file, content := range got {
		expected_content := expected[file]
		if !bytes.HasPrefix(expected_content, content) {
			logger.Warningf("except %v, got %v, len %v , %v", string(expected_content), string(content), len(expected_content), len(content))
			return fmt.Errorf("Content Not Equal %v", file)
		}
	}

	return nil
}
