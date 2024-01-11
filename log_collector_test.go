package llogtail

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
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
	collector.Run()
	gexpected := make(map[string][]byte) // path -> data
	ggot := make(map[string][]byte)

	t.Run("Test Collector Collect", func(t *testing.T) {
		// mock data
		expected := make(map[string][]byte)
		for i := 0; i < 100; i++ {
			mock := generateDataOneKB()
			path := kLogs[i%3]
			seq := expected[path]

			// logger.Debugf("mock -> %v to %v", string(mock[:10]), path)
			seq = append(seq, mock...)
			expected[path] = seq
			appendf(path, mock)
		}
		sconf, err := readFileSinkConf("./tests/sink.json")
		if err != nil {
			logger.Errorf("read sink conf -> %w", err)
			t.FailNow()
		}
		time.Sleep(time.Duration(conf.Watcher.FilterInterval) * time.Second)

		got, _ := kGetCollectorContent(sconf.Dst)
		gexpected = kJoinData(gexpected, expected)
		ggot = kJoinData(ggot, got)

		if err := kCheckContent(expected, ggot); err != nil {
			logger.Errorf("check content -> %v", err.Error())
			t.FailNow()
		}

	})

	t.Run("Test Collector Rename Collect", func(t *testing.T) {
		expected := make(map[string][]byte)
		for i := 0; i < 10000; i++ {
			mock := generateDataOneKB()
			path := kLogs[i%3]
			seq := expected[path]

			// logger.Debugf("mock -> %v to %v", string(mock[:10]), path)
			seq = append(seq, mock...)
			expected[path] = seq
			appendf(path, mock)

			if rand.Int31()%4 == 1 {
				err := rotate(path)
				if err != nil {
					panic(err)
				}
				logger.Debug(path, "ratate")
			}
		}
		sconf, err := readFileSinkConf("./tests/sink.json")
		if err != nil {
			logger.Errorf("read sink conf -> %w", err)
			t.FailNow()
		}

		got, _ := kGetCollectorContent(sconf.Dst)
		gexpected = kJoinData(gexpected, expected)
		ggot = kJoinData(ggot, got)

		if err := kCheckContent(gexpected, ggot); err != nil {
			logger.Errorf("check content -> %v", err.Error())
			t.FailNow()
		}
	})
	// collector.Close()
	// t.Run("Test Collector Restart", func(t *testing.T) {
	// 	collector = nil
	// 	c := NewLogCollector()
	// 	defer collector.Close()
	// 	time.Sleep(time.Second)
	// 	c.Init(*conf)
	// 	c.Run()

	// 	expected := make(map[string][]byte)
	// 	for i := 0; i < 10000; i++ {
	// 		mock := generateDataOneKB()
	// 		path := kLogs[i%3]
	// 		seq := expected[path]

	// 		// logger.Debugf("mock -> %v to %v", string(mock[:10]), path)
	// 		seq = append(seq, mock...)
	// 		expected[path] = seq
	// 		appendf(path, mock)
	// 	}
	// 	sconf, err := readFileSinkConf("./tests/sink.json")
	// 	if err != nil {
	// 		logger.Errorf("read sink conf -> %w", err)
	// 		t.FailNow()
	// 	}

	// 	got, _ := kGetCollectorContent(sconf.Dst)
	// 	gexpected = kJoinData(gexpected, expected)
	// 	ggot = kJoinData(ggot, got)

	// 	if err := kCheckContent(gexpected, ggot); err != nil {
	// 		logger.Errorf("check content -> %v", err.Error())
	// 		t.FailNow()
	// 	}
	// })
}

func kGetCollectorContent(target string) (map[string][]byte, error) {
	fd, err := os.Open(target)
	if err != nil {
		return nil, fmt.Errorf("open %v -> %w", target, err)
	}
	defer fd.Close()
	got := make(map[string][]byte)
	scanner := bufio.NewScanner(fd)
	unit := kDataUnit{}
	for scanner.Scan() {
		line := scanner.Text()
		if err := json.Unmarshal(UnsafeStringToSlice(line), &unit); err != nil {
			return nil, fmt.Errorf("unmarshal line to uint -> %w", err)
		}
		got[unit.File] = append(got[unit.File], unit.Content...)
	}
	return got, nil
}

func kCheckContent(expected map[string][]byte, got map[string][]byte) error {
	for file, content := range got {
		expected_content := expected[file]
		if !bytes.Equal(expected_content, content) {
			logger.Warningf("except %v, got %v, len %v , %v", string(expected_content), string(content), len(expected_content), len(content))
			return fmt.Errorf("Content Not Equal %v", file)
		}
	}

	return nil
}

func kJoinData(a, b map[string][]byte) map[string][]byte {
	for k, v := range b {
		if av, ok := a[k]; ok {
			av = append(av, v...)
		} else {
			a[k] = v
		}
	}
	return a
}
