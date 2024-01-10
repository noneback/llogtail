package llogtail

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

type FileSink struct {
	Sink
	dst   string
	fd    *os.File
	mutex *sync.Mutex
}

type kLogDataUint struct {
	Content []byte `json:"content"`
	Path    string `json:"path"`
}

func NewFileSink() Sink {
	return &FileSink{
		mutex: &sync.Mutex{},
	}
}

type FileSinkConf struct {
	Dst string `json:"dst"`
}

func readFileSinkConf(path string) (*FileSinkConf, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("open %v -> %w", path, err)
	}
	var conf FileSinkConf
	if err := json.Unmarshal(content, &conf); err != nil {
		return nil, fmt.Errorf("unmarshal -> %w", err)
	}
	return &conf, nil
}

func (s *FileSink) Open(path string) error {
	conf, err := readFileSinkConf(path)
	if err != nil {
		return fmt.Errorf("read conf -> %w", err)
	}
	s.dst = conf.Dst
	s.fd, err = os.OpenFile(s.dst, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("open dst %v -> %w", s.dst, err)
	}
	return nil
}

func (s *FileSink) Push(fpath string, content []byte) error {
	defer ExectionTimeCost(fmt.Sprintf("Pust -> %v", s.dst), time.Now())
	s.mutex.Lock()
	defer s.mutex.Unlock()

	logger.Infof("file sink: %v", UnsafeSliceToString(content[:10]))

	content, err := json.Marshal(&kDataUnit{
		File:    fpath,
		Content: content,
	})
	if err != nil {
		return fmt.Errorf("masrshal data unit -> %w", err)
	}

	n, err := s.fd.Write(append(content, []byte("\n")...))
	if err != nil {
		return fmt.Errorf("write %v -> %w", s.dst, err)
	}

	logger.Infof("Pushed %v bytes -> %v", n, s.dst)
	return nil
}

func (s *FileSink) Close() error {
	s.fd.Close()
	return nil
}
