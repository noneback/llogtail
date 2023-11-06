package llogtail

// import (
// 	"bytes"
// 	"errors"
// 	"fmt"
// 	"os"
// 	"path/filepath"
// 	"sync"
// 	"testing"
// 	"time"

// 	"github.com/fsnotify/fsnotify"
// )

// func TestMd5(t *testing.T) {
// 	path := "sfas"
// 	fmt.Println(genCptPath(path))
// }

// func TestRegex(t *testing.T) {
// 	pattern := "ha.log"
// 	strs := []string{
// 		"ha.log.qw44e",
// 		"a3ha.log",
// 		"ha.log",
// 	}

// 	for _, str := range strs {
// 		fmt.Println(filepath.Match(pattern, str))
// 	}
// }

// type fdwrap struct {
// 	fd *os.File
// }

// func TestInotify(t *testing.T) {
// 	path := "./test/error.log"
// 	os.Create(path)
// 	watcher, _ := fsnotify.NewWatcher()
// 	wg := sync.WaitGroup{}
// 	watcher.Add(path)
// 	fd, err := os.Open(path)
// 	if err != nil {
// 		panic(err)
// 	}
// 	fdw := &fdwrap{fd}
// 	eventC := make(chan fdwrap)
// 	closeC := make(chan struct{})
// 	go func() {
// 		for {
// 			select {
// 			case e := <-watcher.Events:
// 				fmt.Println("event", e.Op, e.Name)
// 			case <-closeC:
// 				return
// 			}
// 		}
// 	}()
// 	os.Rename(path, path+".NEW")
// 	os.Remove(path + ".NEW")

// 	go func() {
// 		wg.Add(1)
// 		defer wg.Done()
// 		fdw := <-eventC
// 		fmt.Println("after 3s")
// 		err = fdw.fd.Close()
// 		fmt.Printf("t: %v\n", err)
// 	}()
// 	go func() {
// 		wg.Add(1)
// 		defer wg.Done()
// 		eventC <- (*fdw)
// 		err = fdw.fd.Close()
// 		fmt.Printf("t: %v\n", err)
// 	}()
// 	wg.Wait()
// 	err = fdw.fd.Close()
// 	fmt.Printf("t: %v\n", err)
// 	wait()
// 	time.Sleep(time.Second)
// 	defer watcher.Close()
// 	closeC <- struct{}{}
// }

// func TestLastId(t *testing.T) {
// 	data := []byte("2023-01-30 16:01:14.720 [grpc-biz-9] INFO  - custins 1059575 defer 120s, type: maintenance\nsfsafsa")
// 	n := 0
// 	lineSep := "\n"
// 	idx := bytes.LastIndex(data[:94], []byte(lineSep))
// 	n = idx + 1 // if not found idx = -1, n = 0, else n = idx + 1
// 	fmt.Println(n, idx, len(data))
// }

// func TestBufferNoProgress(t *testing.T) {
// 	buf := NewBlockingBuffer(4 * KB)
// 	file, err := os.Open("/root/workspace/UniverseExplorer/common/collect/universe-2023-04-07T02-30-28.224.log")
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer file.Close()
// 	go func() {
// 		for {
// 			time.Sleep(time.Second)
// 			fmt.Println("ttt", buf.full)
// 			fmt.Println("ttt", buf.Fetch()[:10])
// 		}
// 	}()
// 	for {
// 		time.Sleep(time.Millisecond * 100)
// 		n, err := buf.ReadLinesFrom(file, "\n")
// 		if err != nil {
// 			testlog.Println(n, err)
// 			// file.Seek(0, 0)
// 		}
// 		testlog.Println(n)

// 	}

// }

// func TestErrIs(t *testing.T) {
// 	err := func() error {
// 		return ErrNoProgress
// 	}()

// 	fmt.Println(errors.Is(err, ErrNoProgress))
// }
