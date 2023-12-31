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
	shell := `rm -rf tests && mkdir -p tests && cd tests && touch error.log warn.log info.log`
	_, err := exec.Command("sh", "-c", shell).Output()
	if err != nil {
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
			PollerInterval: time.Second * 3,
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
	logs := []string{
		"tests/error.log", "tests/info.log", "tests/warn.log",
	}
	t.Run("Test Watch Modify Event", func(t *testing.T) {
		for _, logf := range logs {
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
		for _, logf := range logs {
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
		for _, logf := range logs {
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
		for _, logf := range logs {
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

	t.Run("Test Watcher Poller", func(t *testing.T){
		Pre()
		logger.Notice("Sleep for Poller")
		time.Sleep(4 * time.Second)

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
	data := make([]byte, 1024) // 1KB = 1024 Bytes
	for i := range data {
		data[i] = byte(rand.Intn(256)) // Random byte value between 0-255
	}
	return data
}

// modify 文件内容修改函数
func modify(path string, newData []byte) {
	if err := os.WriteFile(path, newData, 0644); err != nil {
		logger.Errorf("modify %v", path)
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
	newLogName := fmt.Sprintf("%s.%d-%02d-%02dT%02d-%02d-%02d",
		logPath, currentTime.Year(), currentTime.Month(), currentTime.Day(),
		currentTime.Hour(), currentTime.Minute(), currentTime.Second())

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


