package llogtail

import (
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/eapache/queue"
	"github.com/fsnotify/fsnotify"
	"github.com/op/go-logging"
)

var logger *logging.Logger
var defauleLogOption = LogOption{
	Verbose: true,
	Level:   logging.INFO,
}

func init() {
	InitLogger(&defauleLogOption)
}

func genFileSign(file *os.File) (*[16]byte, error) {
	sign := make([]byte, SignContentSize)
	if _, err := file.Read(sign); err != nil {
		if !errors.Is(err, io.EOF) {
			file.Close()
			return nil, fmt.Errorf("[genFileSign] read file %v -> %w", file.Name(), err)
		}
	}
	hash := md5.Sum(sign)
	return &hash, nil
}

func findFiles(dir, pattern string) ([]fs.FileInfo, []string, error) {
	matchedLogs := make([]fs.FileInfo, 0)
	depth := 1 + strings.Count(pattern, string(os.PathSeparator))
	pattern = filepath.Base(pattern)

	// find all matched log
	pathList, err := WalkDirs(dir, depth, func(p string) bool {
		flag, err := filepath.Match(pattern, p)
		if err != nil {
			return false
		}
		return flag
	}, func(fs fs.FileInfo) {
		matchedLogs = append(matchedLogs, fs)
	})
	if err != nil {
		return nil, nil, fmt.Errorf("findFile walk dir %v failed -> %w", dir, err)
	}
	if len(matchedLogs) == 0 {
		return nil, nil, fmt.Errorf("findFile No Matched File in Dir %v with Pattern %v, collect stop", dir, pattern)
	}

	return matchedLogs, pathList, nil
}

func findFileByPath(path string) (fs.FileInfo, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("findFileByPath -> %w", err)
	}
	return fi, err
}

// detectNewFile detect whether the file of path is a new file based on its dev + inode + sign.
// fInfo is new file info
// NOTE: be careful, may cause fd leak
func detectNewFile(path string, fInfo os.FileInfo, old *LogMeta) (*FileMeta, error) {
	stat, ok := fInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, fmt.Errorf("[LogWatcher] register file, file stat failed, Not a syscall.Stat_t")
	}
	fd, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("[handleEvent] open file %v -> %w", path, err)
	}

	hash, err := genFileSign(fd) // close fd is return a err
	if err != nil {
		fd.Close()
		return nil, fmt.Errorf("[handleEvent] gen file sign -> %w", err)
	}
	if stat.Dev == old.fMeta.Dev && stat.Ino == old.fMeta.Inode && *hash == old.fMeta.Hash {
		fd.Close()
		return nil, nil
	}
	//TODO: do we need to close the old one? Upper layer should be responsible for that.
	newFileMeta := &FileMeta{
		fd:    fd,
		Inode: stat.Ino,
		Dev:   stat.Dev,
		Hash:  *hash,
	}
	log.Printf("detectNewFile, file %v, inode %v\n", path, stat.Ino)
	return newFileMeta, nil
}

func eventTransform(opkind fsnotify.Op) LogFileEvent {
	transformer := map[fsnotify.Op]LogFileEvent{
		fsnotify.Rename: LogFileRenameRotate,
		fsnotify.Remove: LogFileRemove,
		fsnotify.Write:  LogFileModify,
		fsnotify.Chmod:  LogFileChomd,
	}
	if t, ok := transformer[opkind]; ok {
		return t
	}

	return LogFileEventNotEncoded
}

func validateCpt(cpt *kCheckpoint, meta *LogMeta) bool {
	if cpt.Meta.Dev == meta.fMeta.Dev && cpt.Meta.Inode == meta.fMeta.Inode && cpt.Offset <= uint64(meta.LogInfo.Size()) { // TODO: hash
		return true
	}
	return false
}

// WalkDirs walks dir with depth, and filter matched dir
// filter,p is dir or file name
// hook is called when a file is matched
func WalkDirs(dir string, depth int, filter func(p string) bool, hook func(fs fs.FileInfo)) ([]string, error) {
	matchedDirs := make([]string, 0, 50)
	level := 1
	q := queue.New() // only store dir path
	q.Add(dir)
	q.Add("")

	for q.Length() > 0 && level <= depth {
		dir = q.Remove().(string)
		if len(dir) == 0 {
			// reach dir level end
			level++
			q.Add("")
			continue
		}

		infos, err := ioutil.ReadDir(dir)
		if err != nil {
			return matchedDirs, err
		}

		for _, info := range infos {
			path := filepath.Clean(dir + string(os.PathSeparator) + info.Name())
			if level == depth && filter(info.Name()) {
				matchedDirs = append(matchedDirs, path) // name has some problem
				if hook != nil {
					hook(info)
				}
			}
			if level < depth && info.IsDir() {
				q.Add(path) // put dir info into queue
			}
		}
	}
	return matchedDirs, nil
}

func ExectionTimeCost(title string, start time.Time) {
	msg := fmt.Sprintf("[%v] time cost: %v", title, time.Since(start))
	logger.Notice("[ExectionTimeCost] ", msg)
}

func Retry(times int, interval time.Duration, method func() error) error {
	for i := 1; i <= times; i++ {
		err := method()
		if err == nil {
			return nil
		}
		if i == times {
			return fmt.Errorf("retries %v times, still failed -> %w", times, err)
		}
		time.Sleep(interval)
	}
	return nil
}

type LogOption struct {
	Verbose bool
	Level   logging.Level
}

func InitLogger(opt *LogOption) {
	if !opt.Verbose {
		return
	}

	var once sync.Once
	once.Do(
		func() {
			logging.SetFormatter(logging.MustStringFormatter(
				`%{time:2006-01-02 15:04:05} %{color}%{shortfunc} ▶ %{level:.8s}%{color:reset} %{message}`,
			))
			logging.SetLevel(opt.Level, "")
			logger = logging.MustGetLogger("llogtail")
		},
	)
}

// read checkpoint and store it in cpts
// TODO: add reader queue into checkpoint
func readCheckpoint(path string) (*kCheckpoint, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("readCheckpoint ReadFile %v -> %w", path, err)
	}
	cpt := &kCheckpoint{}
	if err := json.Unmarshal(content, cpt); err != nil {
		return nil, fmt.Errorf("readCheckpoint Unmarshal %v -> %w", path, err)
	}
	return cpt, nil
}

// makeCheckpoint gen checkpoint file or read a original one
// and also responsiable cpts in LogCollector
// TODO: put reader queue into checkpoint
func makeCheckpoint(path string, meta *FileMeta, offset uint64) (*kCheckpoint, error) {
	file, err := os.Create(path) // create a new one or truncate file
	cpt := &kCheckpoint{*meta, uint64(offset), meta.fd.Name()}
	if err != nil {
		return nil, fmt.Errorf("create or open checkpoint file %v -> %w", path, err)
	}
	defer file.Close()
	content, err := json.Marshal(cpt)
	if err != nil {
		return nil, fmt.Errorf("Marshal -> %w", err)
	}
	// create a new file
	if err := os.WriteFile(path, content, os.ModeAppend); err != nil {
		return nil, fmt.Errorf("write cpt file %v -> %w", path, err)
	}
	logger.Infof("makeCheckpoint success, path %v, file %v", path, meta.fd.Name())
	return cpt, nil
}

func UnsafeSliceToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// zero-copy slice convert to string
func UnsafeStringToSlice(s string) (b []byte) {
	p := unsafe.Pointer((*reflect.StringHeader)(unsafe.Pointer(&s)).Data)
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	hdr.Data = uintptr(p)
	hdr.Cap = len(s)
	hdr.Len = len(s)
	return b
}
