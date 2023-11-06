package llogtail

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"

	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"
)



func writeOneK(file *os.File) {
	if n, err := file.Write(oneKBLine); err != nil {
		testlog.Fatalf("[Write One M] %v", err)
	} else {
		testlog.Printf("WriteOneM write %v byte, file name %v", n, file.Name())
	}

}

func WriteOneK(close chan struct{}, file *os.File) {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ticker.C:
			if n, err := file.Write(oneKBLine); err != nil {
				testlog.Fatalf("[Write One K ] %v", err)
			} else {
				testlog.Printf("WriteOneK write %v byte", n)
			}
		case <-close:
			return
		}

	}
}

func TestCollectOnce(t *testing.T) {
	c := makeConfig()
	c.prepare()
	c.banner("#######", "TestCollectOnce")
	path := "/root/workspace/UniverseExplorer/common/collect/test/error.log"
	file, err := os.OpenFile(path, os.O_RDWR, os.ModeAppend)
	if err != nil {
		panic(err)
	}
	c.prepare()
	makeLine(oneKBLine, byte('a'+rand.Int31n(26)))
	writeOneK(file)
	writeOneK(file)
	writeOneK(file)
	writeOneK(file)
	writeOneK(file)
	file.Close()

	c.banner("******", "Test Collect Once Begin")
	if err := c.collector.Init(c.conf); err != nil {
		testlog.Printf("[TestCollectOnce] Init collector %v", err)
		t.FailNow()
	}
	time.Sleep(time.Second * 3)
	running := true
	go c.collectToFile(&running)
	wait()
	c.collector.Close()
	running = false
}

func TestCollectMulti(t *testing.T) {
	c := makeConfig()
	c.prepare()
	c.banner("#######", "TestCollect Multiple File Collection")
	c.banner("******", "Test Collect Once Begin")
	if err := c.collector.Init(c.conf); err != nil {
		testlog.Printf("[TestCollectOnce] Init collector %v", err)
		t.FailNow()
	}
	time.Sleep(time.Second)

	for _, path := range c.collector.watcher.pathMap {
		testlog.Println(path)
		// path := "/root/workspace/UniverseExplorer/common/collect/test/error.log"
		file, err := os.OpenFile(path, os.O_RDWR, os.ModeAppend)
		if err != nil {
			panic(err)
		}
		ch := byte('a' + rand.Int31n(26))
		makeLine(oneKBLine, ch)
		testlog.Println("path", path, string(ch))
		for i := 0; i < 5; i++ {
			writeOneK(file)
		}
		file.Close()
	}

	time.Sleep(time.Second * 4)
	running := true
	go c.collectToFile(&running)
	wait()
	c.collector.Close()
	running = false
}

func (c *config) collectToFile(running *bool) {
	file, err := os.OpenFile("./test/collect.out", os.O_TRUNC|os.O_RDWR|os.O_CREATE, os.ModeAppend)
	for *running {
		time.Sleep(1 * time.Second)
		testlog.Println("collected data")
		content2, err2 := c.collector.Collect()
		if err2 != nil {
			panic(err)
		}
		testlog.Println("GetCollectedData", len(content2))
		if len(content2) == 0 {
			continue
		}
		fmt.Println(string(content2[:20]))
		n, err := file.Write(content2)
		fmt.Println("n", n)
		testlog.Println("write collected data", n, err)
	}
	file.Close()
}

func TestCollectEvent(t *testing.T) {
	c := makeConfig()
	c.prepare()
	c.banner("#######", "TestCollect File By Event ")
	c.banner("******", "Test Collect Begin")
	if err := c.collector.Init(c.conf); err != nil {
		testlog.Printf("[TestCollectOnce] Init collector %v", err)
		t.FailNow()
	}
	time.Sleep(time.Second)
	// first write file
	for _, path := range c.collector.watcher.pathMap {
		testlog.Println(path)
		file, err := os.OpenFile(path, os.O_RDWR, os.ModeAppend)
		if err != nil {
			panic(err)
		}
		ch := byte('a' + rand.Int31n(26))
		makeLine(oneKBLine, ch)
		testlog.Println("path", path, string(ch))
		for i := 0; i < 5; i++ {
			writeOneK(file)
		}
		file.Close()
	}

	running := true
	go c.collectToFile(&running)
	// time.Sleep(3 * time.Second)
	// gen some event
	events := []LogFileEvent{
		// LogFileDiscover,
		LogFileModify,
		LogFileRenameRotate,
		LogFileModify,
		LogFileModify,
		// LogFileRemove,
		// LogFileModify,
		// LogFileModify,
	}
	// gen event
	for _, path := range c.collector.watcher.pathMap {
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
			genEventV2(e, old, new, old, c)
		}
		fd.Close()
	}

	wait()
	c.collector.Close()
	running = false
}

func genEventV2(e LogFileEvent, old, new, path string, c *config) {
	c.banner("-------", "Event for "+old+","+new+","+path)
	switch e {
	case LogFileModify:
		file, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
		if n, err := file.Write(oneKBLine); err != nil || n != KB {
			testlog.Fatalf("[genEvent] LogFileModify Write File err %v or n != KB", err)
		}
		file.Close()
	case LogFileRenameRotate:
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
	case LogFileRemove:
		if err := os.Remove(new); err != nil {
			testlog.Fatalf("[genEvent] LogFileRenameRotate Remove %v -> %v", old, err)
		}
	case LogFileDiscover:
	default:
		testlog.Println("not support", e)
	}
}

func TestBuffer(t *testing.T) {
	buf := NewBlockingBuffer(1024)
	file, _ := os.Open("./test/error.log")
	// make it full
	n, err := buf.ReadLinesFrom(file, "\n")
	if err != nil {
		testlog.Fatalln("read line from file", n, err)
	}

	go func() {
		buf.IfFullThenWait()
	}()
	time.Sleep(2 * time.Second)
	testlog.Println(buf.Fetch())
}

func TestCollectResume(t *testing.T) {
	c := makeConfig()
	// c.prepare()
	c.banner("#######", "TestCollect Resume File Collection")
	c.banner("******", "Test Collect Resume Begin")
	if err := c.collector.Init(c.conf); err != nil {
		testlog.Printf("[TestCollectOnce] Init collector %v", err)
		t.FailNow()
	}
	time.Sleep(time.Second)
	// first write file
	for _, path := range c.collector.watcher.pathMap {
		testlog.Println(path)
		file, err := os.OpenFile(path, os.O_RDWR, os.ModeAppend)
		if err != nil {
			panic(err)
		}
		ch := byte('a' + rand.Int31n(26))
		makeLine(oneKBLine, ch)
		testlog.Println("path", path, string(ch))
		for i := 0; i < 5; i++ {
			writeOneK(file)
		}
		file.Close()
	}
	running := true
	go c.collectToFile(&running)
	wait()
	c.collector.Close()
	running = false
	time.Sleep(time.Second)
}

func TestCollectFailure(t *testing.T) {
	// 1. pattern is not found, it will restart by upper layer
	// 2. file is removed manually, handle in collector
	c := makeConfig()
	c.prepare()
	c.banner("#######", "TestCollect Failure File Collection")
	c.banner("******", "Test Collect Failure Begin")
	if err := c.collector.Init(c.conf); err != nil {
		testlog.Printf("[TestCollectOnce] Init collector %v", err)
		t.FailNow()
	}
	running := true
	go c.collectToFile(&running)
	// do something
	time.Sleep(time.Second)
	// first write file
	for _, path := range c.collector.watcher.pathMap {
		testlog.Println(path)
		file, err := os.OpenFile(path, os.O_RDWR, os.ModeAppend)
		if err != nil {
			panic(err)
		}
		ch := byte('a' + rand.Int31n(26))
		makeLine(oneKBLine, ch)
		testlog.Println("path", path, string(ch))
		for i := 0; i < 5; i++ {
			writeOneK(file)
		}
		file.Close()
	}
	time.Sleep(time.Second * 2)
	for _, path := range c.collector.watcher.pathMap {
		os.Remove(path)
		time.Sleep(time.Second)
		fd, _ := os.Create(path)
		fd.Close()
	}
	time.Sleep(time.Second * 5)
	// first write file
	for _, path := range c.collector.watcher.pathMap {
		testlog.Println(path)
		file, err := os.OpenFile(path, os.O_RDWR, os.ModeAppend)
		if err != nil {
			panic(err)
		}
		ch := byte('a' + rand.Int31n(26) + 1)
		makeLine(oneKBLine, ch)
		testlog.Println("path", path, string(ch))
		for i := 0; i < 5; i++ {
			writeOneK(file)
		}
		file.Close()
	}
	wait()
	c.collector.Close()
	running = false
	time.Sleep(3 * time.Second)
}

func TestFileRotate(t *testing.T) {
	c := makeConfig()
	c.prepare()
	c.banner("#########", "begin to test file rotate")
	conf := &LogConf{
		Dir:      "/root/workspace/UniverseExplorer/common/collect/",
		Pattern:  "universe.log",
		LineSep:  "\n",
		FieldSep: "\t",
	}
	if err := c.collector.Init(conf); err != nil {
		panic(err)
	}
	testlog.Println(c.collector.watcher.pathMap)
	testlog.Println(c.collector.watcher.logMetas)

	running := true
	go c.collectToFile(&running)
	time.Sleep(time.Second)
	go func() {
		cnt := 0
		for running && cnt <= 2 {
			testlog.Println("write one MB", cnt, time.Now())
			// ilog.Info("test", ilog.String("0.5MB", string(oneMBLine)))
			for i := 0; i < 5*1024; i++ {
				// ilog.Info("test", ilog.String("KB", string(oneKBLine)))
			}
			testlog.Printf("Wait For no Progress")
			time.Sleep(10 * time.Second)
			cnt++
		}
	}()
	wait()
	c.collector.Close()
	running = false
}

func perf() {
	testlog.Println("perf")
	go func() {
		err := http.ListenAndServe(":6080", nil)
		if err != nil {
			panic(err)
		}
	}()
}

func TestInitFailed(t *testing.T) {
	perf()
	c := makeConfig()
	c.banner("#########", "TestInitFailed")
	conf := &LogConf{
		Dir:      "/root/workspace/UniverseExplorer/common/collect/test",
		Pattern:  "error33.log",
		LineSep:  "\n",
		FieldSep: "\t",
	}
	m := make(map[int]interface{})

	testlog.Printf("begin: 协程数量->%d\n", runtime.NumGoroutine())
	runtime.MemProfileRate = 20
	for i := 0; i < 500; i++ {
		c := makeConfig()
		m[i] = c
		if err := c.collector.Init(conf); err != nil {
			testlog.Println("Collecotr init failed", i)
			testlog.Printf("begin close")
			c.Close()
			testlog.Printf("协程数量->%d\n", runtime.NumGoroutine())
		}
		time.Sleep(time.Millisecond * 5)
	}
	// m = make(map[int]interface{})
	time.Sleep(5 * time.Second)
	testlog.Printf("协程数量->%d\n", runtime.NumGoroutine())
	time.Sleep(5 * time.Second)
	testlog.Printf("协程数量->%d\n", runtime.NumGoroutine())

	// wait()
	c.collector.Close()
}

func TestNewBuffer(t *testing.T) {
	perf()
	for i := 0; i < 100; i++ {
		testlog.Println(i)
		b := NewBlockingBuffer(MB)
		file, err := os.Open("./buffer.go")
		if err != nil {
			panic(err)
		}
		b.ReadLinesFrom(file, "\n")
		for j := 0; j < 20; j++ {
			b.enlarge()
		}
		// b.Close()
	}
	wait()
}

func TestPollerReFind(t *testing.T) {
	c := makeConfig()
	c.prepare()
	c.banner("#######", "TestCollect TestPollerReFind")
	c.banner("******", "Test TestPollerReFind Begin")
	if err := c.collector.Init(c.conf); err != nil {
		testlog.Printf("[TestCollectOnce] Init collector %v", err)
		t.FailNow()
	}
	time.Sleep(time.Second)
	rmpath := "/root/workspace/UniverseExplorer/common/collect/test/error.log"
	for _, path := range c.collector.watcher.pathMap {
		testlog.Println(path)
		file, err := os.OpenFile(path, os.O_RDWR, os.ModeAppend)
		if err != nil {
			panic(err)
		}
		ch := byte('a' + rand.Int31n(26))
		makeLine(oneKBLine, ch)
		testlog.Println("path", path, string(ch))
		for i := 0; i < 5; i++ {
			writeOneK(file)
		}
		file.Close()
	}
	// has already write 15 lines
	running := true
	go c.collectToFile(&running)
	time.Sleep(time.Second)
	c.banner("++++++++", "test poller")
	testlog.Println("Before RM inode ", getInode(rmpath))

	err := os.Remove(rmpath)
	if err != nil {
		panic(err)
	}

	f, err := os.Create("/tmp/test")
	if err != nil {
		panic(err)
	}
	f.Close()

	newF, err := os.Create(rmpath)
	if err != nil {
		panic(err)
	}
	testlog.Println("After RM inode ", getInode(rmpath))

	wait()

	// for i := 0; i < 5; i++ {
	// 	time.Sleep(time.Second)
	// 	testlog.Println("times: ")
	// 	ch := byte('a' + rand.Int31n(26))
	// 	makeLine(oneKBLine, ch)
	// 	writeOneK(newF)
	// }
	newF.Close()
	wait()
	c.collector.Close()
	running = false
}

func getInode(path string) uint64 {
	fi, err := os.Stat(path)
	if err != nil {
		panic(err)
	}
	return fi.Sys().(*syscall.Stat_t).Ino
}

func TestNoProgressAndRename(t *testing.T) {
	c := makeConfig()
	c.prepare()
	c.banner("#######", "TestCollect TestPollerReFind")
	c.banner("******", "Test TestPollerReFind Begin")
	if err := c.collector.Init(c.conf); err != nil {
		testlog.Printf("[TestCollectOnce] Init collector %v", err)
		t.FailNow()
	}
	time.Sleep(time.Second)
	renamePath := "/root/workspace/UniverseExplorer/common/collect/test/error.log"
	for _, path := range c.collector.watcher.pathMap {
		testlog.Println(path)
		file, err := os.OpenFile(path, os.O_RDWR, os.ModeAppend)
		if err != nil {
			panic(err)
		}
		ch := byte('a' + rand.Int31n(26))
		makeLine(oneKBLine, ch)
		testlog.Println("path", path, string(ch))
		for i := 0; i < 5; i++ {
			writeOneK(file)
		}
		file.Close()
	}
	// has already write 15 lines
	running := true
	go c.collectToFile(&running)
	testlog.Printf("Wait For no progress")
	wait()
	testlog.Printf("Start to Rename Rotate")
	if err := os.Rename(renamePath, renamePath+".tmp"); err != nil {
		panic(err)
	}
	f, err := os.Create(renamePath)
	if err != nil {
		panic(err)
	}
	n, err := f.WriteString("After Rename\n")
	testlog.Printf("n %v, err %v", n, err)
	f.Close()
	if err := os.Remove(renamePath + ".tmp"); err != nil {
		panic(err)
	}
	wait()
}
