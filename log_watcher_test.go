package llogtail

import (
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/eapache/queue"
)

var kWatcherCloseC chan struct{}

const kPoller = 3 * time.Second

var kLogs = []string{
	"tests/error.log", "tests/info.log", "tests/warn.log",
}

type kSimEvent struct {
	path string
	// inode int
	e LogFileEvent
}

type EventReceiver struct {
	eventQ map[string]*queue.Queue
	closeC chan struct{}
	eventC chan *Event
}

func NewEventReceiver(eventC chan *Event) *EventReceiver {
	return &EventReceiver{
		eventQ: make(map[string]*queue.Queue),
		closeC: make(chan struct{}),
		eventC: eventC,
	}
}

func (er *EventReceiver) Run() {
	logger.Notice("Receive Backgroud Task Start")
	go func() {
		for {
			select {
			case e := <-er.eventC:
				logger.Info("ReceiveQ <= ", e.String())
				if _, ok := er.eventQ[e.meta.path]; !ok {
					er.eventQ[e.meta.path] = queue.New()
				}
				er.eventQ[e.meta.path].Add(e)
			case <-er.closeC:
				logger.Notice("Receive Backgroud Task Exit")
				return
			}
		}
	}()
}

func (er *EventReceiver) Draining() {
	for _, v := range er.eventQ {
		for v.Length() != 0 {
			v.Remove()
		}
	}
}

func (er *EventReceiver) Match(events []kSimEvent) error {
	for _, kEvent := range events {
		q, ok := er.eventQ[kEvent.path]
		if !ok {
			return fmt.Errorf("Path %v not found in Receiver", kEvent.path)
		}
		if q.Length() <= 0 {
			return fmt.Errorf("EventQ %v Empty, expect %v", kEvent.path, kEvent.e)
		}

		event, _ := q.Remove().(*Event)
		if event.e != kEvent.e {
			return fmt.Errorf("%v, expect %v, actual %v", kEvent.path, kEvent, event.e)
		}
	}

	return nil
}

func (er *EventReceiver) Stop() {
	er.closeC <- struct{}{}
}

func Pre() {
	InitLogger(&defauleLogOption)
	shell := `rm -rf tests offset && mkdir -p tests offset && cd tests && touch error.log warn.log info.log collector.json sink.json && echo '{
	"dir": "./tests",
	"pattern": "*.log",
	"lineSeperator": "\\n",
	"sink": {
		"type": "file",
		"config": "tests/sink.json"
	},
	"watcher": {
		"filter": 1,
		"poller": 3
	}
}' > collector.json && echo '{ "dst": "./tests/dst.txt" }' > sink.json`
	_, err := exec.Command("sh", "-c", shell).Output()
	if err != nil {
		logger.Errorf("shell %v", shell)
		panic("Pre -> " + err.Error())
	}
	logger.Notice("Pre Success")
}

func TestLogWatcher(t *testing.T) {
	Pre()
	dir, pattern := "tests", "*.log"
	var receiver *EventReceiver

	watcher := NewLogWatcher(
		&LogWatchOption{
			FilterInterval: time.Microsecond * 300,
			PollerInterval: kPoller,
		},
	)

	t.Run("Test Watcher Init", func(t *testing.T) {
		if err := watcher.Init(); err != nil {
			logger.Errorf("Watcher Init -> %w", err)
			t.FailNow()
		}
	})
	receiver = NewEventReceiver(watcher.EventC)
	receiver.Run()
	watcher.RunEventHandler()
	defer receiver.Stop()
	defer watcher.Close()

	t.Run("Test Watcher RegisterAndWatch", func(t *testing.T) {
		defer receiver.Draining()
		if err := watcher.RegisterAndWatch(dir, pattern); err != nil {
			logger.Errorf("Watcher RegisterAndWatch -> %w", err)
			t.FailNow()
		}
		time.Sleep(time.Microsecond * 200)
		expected := []kSimEvent{
			{
				"tests/error.log", LogFileDiscover,
			}, {
				"tests/info.log", LogFileDiscover,
			}, {
				"tests/warn.log", LogFileDiscover,
			},
		}
		if err := receiver.Match(expected); err != nil {
			logger.Error("Event not Match", err)
			t.FailNow()
		}
	})

	t.Run("Test Watch Modify Event", func(t *testing.T) {
		defer receiver.Draining()
		for _, logf := range kLogs {
			modify(logf, kDataOneKB)
		}
		time.Sleep(time.Millisecond * 100)
		if err := receiver.Match([]kSimEvent{
			{
				"tests/error.log", LogFileModify,
			}, {
				"tests/info.log", LogFileModify,
			}, {
				"tests/warn.log", LogFileModify,
			},
		}); err != nil {
			logger.Error("Event not Match", err)
			t.FailNow()
		}
	})

	t.Run("Test Watch Rename Rorate Event", func(t *testing.T) {
		defer receiver.Draining()
		for _, logf := range kLogs {
			// modify(logf,kDataOneKB)
			rotate(logf)
		}
		time.Sleep(time.Millisecond * 100)
		if err := receiver.Match([]kSimEvent{
			{
				"tests/error.log", LogFileRenameRotate,
			}, {
				"tests/info.log", LogFileRenameRotate,
			}, {
				"tests/warn.log", LogFileRenameRotate,
			},
		}); err != nil {
			logger.Error("Event not Match", err)
			t.FailNow()
		}
	})

	t.Run("Test Watch Modify After Rename Rorate Event", func(t *testing.T) {
		defer receiver.Draining()
		for _, logf := range kLogs {
			modify(logf, kDataOneKB)
		}
		time.Sleep(time.Millisecond * 100)
		if err := receiver.Match([]kSimEvent{
			{
				"tests/error.log", LogFileModify,
			}, {
				"tests/info.log", LogFileModify,
			}, {
				"tests/warn.log", LogFileModify,
			},
		}); err != nil {
			logger.Error("Event not Match", err)
			t.FailNow()
		}
	})

	t.Run("Test Watch Remove Event", func(t *testing.T) {
		defer receiver.Draining()
		for _, logf := range kLogs {
			remove(logf)
		}
		time.Sleep(time.Millisecond * 100)
		if err := receiver.Match([]kSimEvent{
			{
				"tests/error.log", LogFileRemove,
			}, {
				"tests/info.log", LogFileRemove,
			}, {
				"tests/warn.log", LogFileRemove,
			},
		}); err != nil {
			logger.Error("Event not Match", err)
			t.FailNow()
		}
	})

	t.Run("Test Watcher Poller", func(t *testing.T) {
		defer receiver.Draining()
		Pre()
		logger.Notice("Sleep for Poller")
		time.Sleep(kPoller + time.Second)

		if err := receiver.Match([]kSimEvent{
			{
				"tests/error.log", LogFileRenameRotate,
			}, {
				"tests/info.log", LogFileRenameRotate,
			}, {
				"tests/warn.log", LogFileRenameRotate,
			},
		}); err != nil {
			logger.Error("Event not Match", err)
			t.FailNow()
		}
	})

}

var kDataOneKB = generateDataOneKB()

// generateDataOneKB generates 1KB of random data
func generateDataOneKB() []byte {
	rand.NewSource(time.Now().UnixNano())
	data := make([]byte, 1023) // 1KB = 1024 Bytes
	ch := byte('a' + rand.Intn(26))
	for i := range data {
		data[i] = ch // Random byte value between 0-255
	}
	data = append(data, '\n')
	return data
}

// modify 文件内容修改函数
func modify(path string, newData []byte) {
	if err := os.WriteFile(path, newData, 0644); err != nil {
		logger.Errorf("modify %v", path)
		panic(err)
	}
}
func appendf(path string, newData []byte) {
	// Open the file in append mode (os.O_APPEND) and write-only mode (os.O_WRONLY)
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		logger.Error("Error:", err)
		panic(err)
	}
	defer file.Close()

	// Write the new data to the file
	_, err = file.Write(newData)
	if err != nil {
		logger.Error("Error:", err)
		panic(err)
	}
}

func rename(oldPath, newPath string) {
	if err := os.Rename(oldPath, newPath); err != nil {
		logger.Errorf("rename %v -> %v", oldPath, newPath)
		panic(err)
	}
}

func remove(path string) {
	if err := os.Remove(path); err != nil {
		logger.Errorf("remove %v -> %v", path)
		panic(err)
	}
}

// rotateLogs 对指定的日志文件进行轮转
func rotate(logPath string) error {
	currentTime := time.Now()
	newLogName := fmt.Sprintf("%s.%d-%02d-%02dT%02d-%02d-%02d-%v",
		logPath, currentTime.Year(), currentTime.Month(), currentTime.Day(),
		currentTime.Hour(), currentTime.Minute(), currentTime.Second(), currentTime.UnixNano())

	err := os.Rename(logPath, newLogName)
	if err != nil {
		return err
	}

	newFile, err := os.Create(logPath)
	if err != nil {
		return err
	}
	newFile.Close()
	return nil
}
