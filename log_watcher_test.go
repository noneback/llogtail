package llogtail

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/op/go-logging"
)

var (
	testlog      *log.Logger
	oneKBLine    []byte
	oneMBLine    []byte
	waitToClear  []clear
	receivedMeta map[LogFileEvent][]*LogMeta
)

type clear struct {
	path string
	fd   *os.File
}

func init() {
	receivedMeta = make(map[LogFileEvent][]*LogMeta)
	testlog = log.Default()
	testlog.SetPrefix("[W] ")
	testlog.SetFlags(log.Lshortfile)
	oneMBLine = make([]byte, MB)
	oneKBLine = make([]byte, KB)
	for i := 0; i < MB; i++ {
		oneMBLine[i] = byte('a')
	}
	for i := 0; i < KB; i++ {
		oneKBLine[i] = byte('a')
		if i+1 == KB {
			oneKBLine[i] = byte('\n')
		}
	}
}

func makeLine(line []byte, ch byte) {
	for i := 0; i < KB; i++ {
		line[i] = byte(ch)
		if i+1 == KB {
			oneKBLine[i] = byte('\n')
		}
	}
}

type WatcherConfig struct {
	running      bool
	watcher      *LogWatcher
	t            *testing.T
	files        []string
	pattern, dir string
	eventC       chan *Event
	closeC       chan struct{}
}

const kTestDir = "./tests"

func (cfg *WatcherConfig) eventReceiver() {
	logger.Info("[Background] Start EventReceiver")
	defer logger.Info("[Background] EventReceiver Exit")
	for cfg.running  {
		select {
		case e := <-cfg.watcher.EventC:
			logger.Info("Receive -> ", e)
		case <-cfg.closeC:
			return
		}
	}
}

func (cfg *WatcherConfig) EventReceiver() {
	go cfg.eventReceiver()
}

func (cfg *WatcherConfig) init() {
	InitLogger(&LogOption{
		true, logging.DEBUG,
	})
	cfg.running = true
	cfg.watcher = NewLogWatcher(&LogWatchOption{
		time.Second * 10, time.Minute * 5,
	})
	if err := cfg.watcher.Init(); err != nil {
		logger.Errorf("LogWatcher ->", err)
		cfg.t.FailNow()
	}

	testFiles := []string{
		"warn.log", "info.log", "error.log",
	}

	for _, f := range testFiles {
		path := filepath.Join(kTestDir, f)
		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			logger.Errorf("[init] OpenFile %v -> %w", path, err)
			continue
		}
		// 关闭文件
		defer f.Close()
	}
	cfg.files = testFiles
	cfg.dir, cfg.pattern = kTestDir, "*.log"

	// start event reveiver
	cfg.EventReceiver()
	cfg.watcher.RunEventHandler()
}

func (cfg *WatcherConfig) doTest() {}

func (cfg *WatcherConfig) close() {
	cfg.watcher.Close()
	cfg.closeC <- struct{}{}
	cfg.running = false
}

func (cfg *WatcherConfig) testRegisterFile() {
	banner("#####", "testRegisterFile start")
	defer banner("#####", "testRegisterFile end")
	if err := cfg.watcher.RegisterAndWatch(kTestDir, cfg.pattern); err != nil {
		logger.Errorf("RegisterAndWatch -> %w", err)
	}
}

func TestMain(t *testing.T) {
	cfg := WatcherConfig{
		t: t,
	}
	cfg.init()
	cfg.testRegisterFile()

	defer cfg.close()
}

func banner(padding string, content string) {
	fmt.Printf("\n[%v %v %v]\n\n", padding, content, padding)
}
