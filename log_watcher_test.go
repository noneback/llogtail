package llogtail

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
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

type config struct {
	running   bool
	conf      *LogConf
	watcher   *LogWatcher
	collector *LogCollector
	wg        *sync.WaitGroup
}

func makeConfig() *config {
	testConf := &config{
		wg: &sync.WaitGroup{},
	}
	conf := &LogConf{
		Dir:      "/root/workspace/UniverseExplorer/common/collect/test",
		Pattern:  "*.log",
		LineSep:  "\n",
		FieldSep: "",
	}
	testConf.conf = conf
	lc := NewLogCollector(nil)
	testConf.collector = lc
	testConf.watcher = &lc.watcher
	return testConf
}

func (c *config) init() {
	if err := c.collector.Init(c.conf); err != nil {
		testlog.Fatalf("[makeConfig] LogCollector Init -> %v", err)
	}
}

func (c *config) Close() {
	c.collector.Close()
}

func (c *config) waitSignal() {
	sigs := make(chan os.Signal, 0)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigs
	fmt.Println(sig.String(), "stopping")
}
func wait() {
	sigs := make(chan os.Signal, 0)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigs
	fmt.Println(sig.String(), "stopping")
}

func genEvent(e LogFileEvent, file *os.File, old, new string, c *config) {
	if file == nil {
		testlog.Fatalln("[genEvent] file is nil")
	}
	switch e {
	case LogFileModify:
		if n, err := file.Write(oneKBLine); err != nil || n != KB {
			testlog.Fatalf("[genEvent] LogFileModify Write File err %v or n != KB", err)
		}
	case LogFileRenameRotate:
		fd := c.watcher.logMetas[old].fMeta.fd
		fd.Close()
		if err := os.Rename(old, new); err != nil {
			testlog.Fatalf("[genEvent] LogFileRenameRotate Rename %v to %v -> %v", old, new, err)
		}
		testlog.Println("rename success", old, new)

		if fd, err := os.Create(old); err != nil {
			testlog.Fatalf("[genEvent] LogFileRenameRotate Create %v -> %v", old, err)
		} else {
			testlog.Println("create success", old)
			fd.Close()
		}
		waitToClear = append(waitToClear, clear{
			path: new,
			fd:   file,
		})

	case LogFileRemove:
		if err := os.Remove(new); err != nil {
			testlog.Fatalf("[genEvent] LogFileRenameRotate Remove %v -> %v", old, err)
		}
	case LogFileDiscover:
	default:
		testlog.Println("not support", e)
	}

}

func (c *config) prepare() {
	registerdLogsSet := map[string]struct{}{
		"error.log": {}, "warning.log": {}, "info.log": {},
	}
	for filename := range registerdLogsSet {
		path := filepath.Join(c.conf.Dir, filename)
		if file, err := os.Create(path); err != nil {
			panic(fmt.Sprintf("[prepare] Create file %v failed,err %v", filename, err))
		} else {
			file.Close()
		}
	}
	os.Mkdir("./offset", os.ModePerm)
	os.Create("./universe.log")
}

func (c *config) clear() {
	c.banner("=======", "clear begin")
	defer c.banner("=======", "clear end")
	for _, clear := range waitToClear {
		if clear.fd != nil {
			clear.fd.Close()
		}
		// if err := os.Remove(clear.path); err != nil {
		// 	testlog.Printf(fmt.Sprintf("[clear] remove file %v, err %v", clear.path, err))
		// }
	}
}

func (c *config) eventReciver(container map[LogFileEvent]struct{}, closeC chan struct{}) {
	watcher := c.watcher
	for {
		select {
		case e, ok := <-watcher.EventC:
			if !ok {
				panic("EventC not ok")
			}
			testlog.Printf("after event %v, current meta: %+v", e.e, watcher.logMetas)
			container[e.e] = struct{}{}
			receivedMeta[e.e] = append(receivedMeta[e.e], e.meta)
		case <-closeC:
			return
		}
	}
}

func (c *config) testWatcherRegisterFiles(t *testing.T) {
	watcher := c.watcher
	registerdLogsSet := map[string]struct{}{
		"error.log": {}, "warning.log": {}, "info.log": {},
	}

	if err := watcher.RegisterAndWatch(c.conf.Dir, c.conf.Pattern); err != nil {
		testlog.Fatalf("[TestWatcher] watcher RegisterAndWatch -> %v", err)
	}
	matched := 0
	for _, path := range watcher.pathMap {
		if _, ok := registerdLogsSet[filepath.Base(path)]; !ok {
			testlog.Fatalf("[TestWatcher] target file %v not found in register files", path)
			t.Fail()
		}
		matched++
	}

	if len(watcher.pathMap) != len(registerdLogsSet) {
		testlog.Fatalf("[TestWatcher] unexpected file registerd, target %v, registered %v", registerdLogsSet, watcher.pathMap)
		t.Fail()
	}

	if matched != len(registerdLogsSet) {
		testlog.Fatalf("[TestWatcher] missing files in register files,target filesSet %v, registerd files %v", registerdLogsSet, watcher.pathMap)
		t.Fail()
	}
	c.banner("*******", "testWatcherRegisterFiles success")
}

func (c *config) banner(padding string, content string) {
	fmt.Printf("\n[%v %v %v]\n\n", padding, content, padding)
}

func (c *config) testWatcherEvent(t *testing.T, closeC chan struct{}, detected map[LogFileEvent]struct{}) {
	c.banner("+++++++", "testWatcherEvent Begin")
	watcher := c.watcher
	events := []LogFileEvent{
		LogFileDiscover,
		LogFileModify,
		LogFileRenameRotate,
		LogFileRemove,
	}
	expectE := map[LogFileEvent]struct{}{
		LogFileDiscover:     {},
		LogFileModify:       {},
		LogFileRenameRotate: {},
		LogFileRemove:       {},
	}
	watcher.RunEventHandler()

	// gen event
	for _, path := range watcher.pathMap {
		fd, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
		if err != nil {
			log.Fatalf("[TestWatcher] OpenFile %v", err)
			t.Fail()
		}
		c.banner("-------", "Event for "+fd.Name())

		old := filepath.Join(c.conf.Dir, filepath.Base(fd.Name()))
		new := old + "." + "2023"
		for _, e := range events {
			time.Sleep(time.Microsecond * 1000 * 3)
			genEvent(e, fd, old, new, c)
		}
		fd.Close()
	}

	// c.clear()

	wait()

	watcher.Close()
	closeC <- struct{}{}
	for event := range expectE {
		if _, ok := detected[event]; !ok {
			log.Printf("missing event detected %v, expected %v", detected, expectE)
			t.FailNow()
		}
	}
}

func (c *config) testRenameEvent(t *testing.T, closeC chan struct{}, detected map[LogFileEvent]struct{}) {
	c.banner("+++++++", "testWatcherEvent Begin")
	watcher := c.watcher
	rename, remove := true, true
	watcher.RunEventHandler()
	oldFds := make([]*os.File, 0)
	pathes := make([]string, 0)
	time.Sleep(time.Second)
	c.banner("*******", "Before Rename")
	for e, metas := range receivedMeta {
		for _, meta := range metas {
			testlog.Printf("event %v, meta, path %v, %+v", e, meta.path, meta.fMeta)
		}
	}

	// gen event
	for _, path := range watcher.pathMap {
		time.Sleep(time.Microsecond * 1000)
		fd, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
		if err != nil {
			log.Fatalf("[TestWatcher] OpenFile %v", err)
			t.Fail()
		}
		c.banner("-------", "Event for "+fd.Name())
		old := filepath.Join(c.conf.Dir, filepath.Base(fd.Name()))
		new := old + "." + "2023"

		if rename {
			fmeta := watcher.logMetas[old].fMeta
			oldFds = append(oldFds, fmeta.fd)
			pathes = append(pathes, new)
			testlog.Println("path", old, "link cnt")
			if err := os.Rename(old, new); err != nil {
				testlog.Fatalf("[genEvent] LogFileRenameRotate Rename %v to %v -> %v", old, new, err)
			}
			testlog.Println("rename success", old, new)

			if fd, err := os.Create(old); err != nil {
				testlog.Fatalf("[genEvent] LogFileRenameRotate Create %v -> %v", old, err)
			} else {
				testlog.Println("create success", old)
				fd.Close()
			}
			waitToClear = append(waitToClear, clear{
				path: new,
				fd:   nil,
			})
		}
		c.banner("*******", "After Rename")
		time.Sleep(time.Second)
		for e, metas := range receivedMeta {
			for _, meta := range metas {
				testlog.Printf("event %v, meta, path %v, %+v", e, meta.path, meta.fMeta)
			}
		}
		watcher.watcher.Remove(old)
		if remove {
			err := os.Remove(new)
			if err != nil {
				panic(err)
			}
		}
		fd.Close()
		c.banner("*******", "After Remove")
		for e, metas := range receivedMeta {
			for _, meta := range metas {
				err := meta.fMeta.fd.Close()
				testlog.Printf("event %v, meta, path %v, %+v, close err %v", e, meta.path, meta.fMeta, err)
			}
		}

	}
	testlog.Println("OldFDS")
	for i, fd := range oldFds {
		if err := fd.Close(); err == nil {
			testlog.Println(pathes[i], "closable")
		} else {
			testlog.Println(pathes[i], "not closable")
		}
	}

	wait()
	time.Sleep(time.Second)
	watcher.Close()
	closeC <- struct{}{}

	log.Println("Detected Event ", detected)
}

func TestWatcher(t *testing.T) {
	c := makeConfig()
	c.prepare()
	// defer c.clear()
	watcher := c.watcher
	c.running = true
	if err := watcher.Init(); err != nil {
		testlog.Fatalf("[TestWatcher] watcher init -> %v", err)
	}

	detected := make(map[LogFileEvent]struct{}) // detected event from eventC
	closeC := make(chan struct{})
	go c.eventReciver(detected, closeC)
	c.testWatcherRegisterFiles(t)
	c.testWatcherEvent(t, closeC, detected)
	// c.testRenameEvent(t, closeC, detected)
}

func TestPoller(t *testing.T) {
	c := makeConfig()
	perf()
	c.prepare()
	// defer c.clear()
	watcher := c.watcher
	c.running = true
	if err := watcher.Init(); err != nil {
		testlog.Fatalf("[TestWatcher] watcher init -> %v", err)
	}

	detected := make(map[LogFileEvent]struct{}) // detected event from eventC
	closeC := make(chan struct{})
	go c.eventReciver(detected, closeC)
	c.testWatcherRegisterFiles(t)
	watcher.RunEventHandler()
	c.banner("+++++++", "Test Poller")
	path := watcher.pathMap[2]
	testlog.Println("path", path)
	os.Remove(path)
	time.Sleep(1000 * time.Microsecond)
	fd, _ := os.Create(path)
	fd.Close()
	wait()
	watcher.Close()
	closeC <- struct{}{}
	testlog.Println(detected)
	time.Sleep(time.Second)
}
