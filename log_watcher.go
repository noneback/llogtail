package llogtail

import (
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
)

const kNotifyEventLimit = 500

type Event struct {
	e    LogFileEvent
	meta *LogMeta
}

func (e Event) String() string {
	return fmt.Sprintf("[ %v, %v, %v ] ", e.e, e.meta.path, e.meta.fMeta.Inode)
}

type LogFileEvent uint64

func (e LogFileEvent) String() string {
	switch e {
	case LogFileRenameRotate:
		return "Log File Rename Rotate"
	case LogFileRemove:
		return "Log File Removed"
	case LogFileModify:
		return "Log File Modify"
	case LogFileDiscover:
		return "Log File Discoverd"
	case LogFileChomd:
		return "Log File Chmod"
	}
	return fmt.Sprintf("Not Encoded Event %b", e)
}

const (
	LogFileRenameRotate = LogFileEvent(1) << iota // rename rotate: rename old log into a archived one, and then create a new file serving log
	LogFileModify
	LogFileChomd
	LogFileRemove
	LogFileDiscover        = LogFileEvent(1) << 62
	LogFileEventNotEncoded = LogFileEvent(1) << 63
	WindowSize             = 32
	SignContentSize        = 1024
)

type LogMeta struct {
	Dir     string
	Pattern string
	path    string
	LogInfo os.FileInfo
	fMeta   *FileMeta
}

type FileMeta struct {
	fd    *os.File
	Inode uint64   `josn:"inode"`
	Dev   uint64   `json:"dev"`
	Hash  [16]byte `json:"hash"`
}

// LogWatcher watch log files based on fsnotify.
// It mainly consists of a event-watcher and a event transform pattern, transform fsnotify event into LogFileEvent.
// Basically, it listens inotify event of log files, transforming into LogFileEvent, and pass to upper collector to cope with. The frequency of sending a event is determined by filterInterval and event kind change.
// It manages two goroutines in event-driven and poller mode. Event-Driven goroutine handle event from fsnotify.Watcher while poller gorotine which is trggered much less frequently, handles operations needing resume after some time, such as removeing a file by mistake.
//
// Commonly speaking, Log File is watched as fellows:
// Step 1, Register File And add into fsnotify watcher. File Pattern supports glob. Note that log file's path is settled, and any other operation related with log's path will only use the determined path, rather than reseatch files.And a Discover Event is generated for each determined log file.
// Step 2, Watcher Events. Event is passed to LogWatcher via a chan. LogWatcher will transform raw event into LogFileEvent based on some pattern(linux only at now) or File's status. Specially, when file is removed, we consider it a mistake and put path into poller quere so that it will be rewatcher when poller triggers.
// Step 3: Send Event.
type LogWatcher struct {
	logMetas       map[string]*LogMeta // file path -> LogMeta？ registerd log file meta
	watcher        *fsnotify.Watcher
	EventC         chan *Event
	closeC         chan struct{}
	pollerCloseC   chan struct{}
	eventFilter    map[string]LogFileEvent // filepath -> event, fifo
	lastSentTs     map[string]time.Time    // fpath -> ts
	filterInterval time.Duration
	pollerInterval time.Duration
	locker         sync.Mutex
	dir, pattern   string
}

type LogWatchOption struct {
	FilterInterval time.Duration
	PollerInterval time.Duration
}

func NewLogWatcher(option *LogWatchOption) *LogWatcher {
	return &LogWatcher{
		EventC:   make(chan *Event),
		logMetas: make(map[string]*LogMeta), // path -> LogMeta
		// windows:        map[string]uint64{},
		closeC:         nil,
		pollerCloseC:   nil,
		lastSentTs:     make(map[string]time.Time),
		eventFilter:    make(map[string]LogFileEvent),
		filterInterval: option.FilterInterval,
		pollerInterval: option.PollerInterval,
	}
}

func (lw *LogWatcher) Init() error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		if watcher != nil {
			watcher.Close()
		}
		return fmt.Errorf("[LogWatcher] Init -> %w", err)
	}
	lw.watcher = watcher
	return nil
}

// start event handler goroutine.
func (lw *LogWatcher) RunEventHandler() {
	go lw.poller()
	go lw.eventHandler()
}

// eventHandler
func (lw *LogWatcher) eventHandler() {
	logger.Notice("[Backgroud] eventHandler Start")
	defer logger.Notice("[Backgroud] eventHandler Exit")

	if lw.closeC == nil {
		lw.closeC = make(chan struct{})
	}

	for {
		select {
		case <-lw.closeC:
			logger.Notice("[eventHandler] close triggered")
			return
		case e, ok := <-lw.watcher.Events:
			if !ok {
				break
			}

			logger.Debugf("LogWatcher watcher.Events %v\n", e)
			if err := lw.handleEvent(e.Name, eventTransform(e.Op)); err != nil {
				logger.Errorf("eventHandler handle event -> %v\n", err)
			}
		case err, ok := <-lw.watcher.Errors:
			if !ok {
				break
			}
			logger.Errorf("LogWatcher meet error %v\n", err.Error())
			return
		}
	}
}

// poller is a goroutine runing in poll mode. it deals with some unexpected situation, such as handle event failure or remove event.
// interval should be larger.
func (lw *LogWatcher) poller() {
	logger.Notice("[Backgroud] poller Start")
	defer logger.Notice("[Backgroud] poller End")

	if lw.pollerCloseC == nil {
		lw.pollerCloseC = make(chan struct{})
	}
	poll_ticker := time.NewTicker(lw.pollerInterval)

	defer poll_ticker.Stop()
	for {
		select {
		case <-poll_ticker.C:
			lw.locker.Lock()
			logger.Debug("poll_ticker trigger")
			// poller based on pattern, path from meta
			logFiles, pathList, err := findFiles(lw.dir, lw.pattern)
			if err != nil {
				logger.Warning("[poller] findFiles failed", err)
				continue
			}

			for i, path := range pathList {
				if meta, ok := lw.logMetas[path]; ok {
					if err := lw.handleRenameRotate(meta); err != nil { // action as it is a rename opt
						logger.Warning("[poller] handle poller task failed", err)
						continue
					} // handle it as a rename event
					logger.Infof("[poller] handle poll_ticker task success, file %v", meta.path)
				} else {
					if err := lw.kRegisterAndWatchSingleFile(path, &logFiles[i]); err != nil {
						logger.Warningf("[poller] register file %v failed %v", path, err)
					}
					logger.Notice("Poller Discover", path)
					lw.sendEvent(LogFileDiscover, lw.logMetas[path])
					logger.Notice("Poller Discover Done", path)
				}
			}

			lw.locker.Unlock()
		case <-lw.pollerCloseC:
			logger.Notice("poller Close")
			return
		}
	}
}

// hanldeRemoved
// key point: if a log is remove by mistake and then re-create manually, we need to register the new log file and replace the old one.
// Commonly, a log do not removed but rather be renamed or copied or truncated
// We just set a removed flag and leave it to upper layer to solve.
func (lw *LogWatcher) handleRemoved(meta *LogMeta) error {
	defer logger.Notice("handle remove", meta.path)
	lw.sendEvent(LogFileRemove, meta)
	return nil
}

// handleRenameRotate
// 1. match and tell a new log file
// 2. update log meta
// 3. send evnet to upper layer
func (lw *LogWatcher) handleRenameRotate(meta *LogMeta) error {
	var (
		fInfo fs.FileInfo
		err   error
	)
	time.Sleep(10 * time.Millisecond) // wait for new file creation
	if err = Retry(3, time.Duration(30*time.Millisecond), func() error {
		fInfo, err = findFileByPath(meta.path)
		return err
	}); err != nil {
		// TODO(noneback): if a rename create file is too slow(lower than 0.1s, not common), do we need to resolve it ?
		return fmt.Errorf("[handleRenameRotate] handle rename rotate failed, relocate log file dir %v, pattern %v -> %w", meta.Dir, meta.Pattern, err)
	}

	path := meta.path
	newFileMeta, err := detectNewFile(path, fInfo, meta)
	if err != nil {
		return fmt.Errorf("[handleRenameRotate] detectNewFile %s -> %w", meta.path, err)
	}
	if newFileMeta == nil {
		return nil // File not change
	}
	lw.watcher.Remove(path) //nolint
	meta.LogInfo = fInfo

	meta.fMeta = newFileMeta
	meta.path = path
	if err := lw.watcher.Add(meta.path); err != nil {
		return fmt.Errorf("[handleRenameRotate] handle rename rotate failed, watch new logfile %s -> %w", meta.path, err)
	}
	logger.Noticef("watcher: %v rotate\n", meta.path)
	lw.sendEvent(LogFileRenameRotate, meta)
	return nil
}

// sendEvent send event to upper layer, NOTICE: a blocking opt
// sendEvent only in eventHandler
func (lw *LogWatcher) sendEvent(e LogFileEvent, meta *LogMeta) {
	lastEvent, ok := lw.eventFilter[meta.path]
	defer func() {
		lw.eventFilter[meta.path] = e
		lw.lastSentTs[meta.path] = time.Now().Local().UTC()
	}()

	if ok && lastEvent == e {
		lastSentTs := lw.lastSentTs[meta.path]
		if len(lw.watcher.Events) >= kNotifyEventLimit { // start filter if notify event piled up
			if time.Since(lastSentTs) < lw.filterInterval { // can be optimized by making a imprecise interval [interval-1，interval + 1], but current having it precisely.
				// less than interval and same as last sent event, do not send
				logger.Debug("sendEvent, event lasting, but not trigger")
				return
			}
		}
	}
	sendMeta := *meta // upper layer is using that address, which may change val in meta.To prevent implicit meta change, we send a copied one.
	// event change or sent time triggered
	lw.EventC <- &Event{e, &sendMeta} // TODO(link.xk): optimize it for update event
}

func (lw *LogWatcher) handleEvent(path string, e LogFileEvent) error {
	meta, ok := lw.logMetas[path]
	if !ok {
		return fmt.Errorf("[handleEvent] Log Meta not found,path: %v", path)
	}
	switch e {
	case LogFileRenameRotate:
		if err := lw.handleRenameRotate(meta); err != nil {
			return fmt.Errorf("[handleEvent] LogFileRenameRotate -> %w", err)
		}
	case LogFileRemove:
		if err := lw.handleRemoved(meta); err != nil {
			return fmt.Errorf("[handleEvent] LogFileRemove -> %w", err)
		}
	case LogFileModify:
		lw.sendEvent(LogFileModify, meta)
	case LogFileChomd:
		// we consider chmod a remove event
		if _, err := os.Stat(meta.path); errors.Is(err, fs.ErrNotExist) {
			if err := lw.handleRemoved(meta); err != nil {
				return fmt.Errorf("[handleEvent] LogFileRemove -> %w", err)
			}
		}

	case LogFileEventNotEncoded:
		logger.Errorf("LogFileEventNotEncoded: %v", e)
		lw.sendEvent(LogFileEventNotEncoded, meta)
	}
	return nil
}

func genLogMeta(logFile fs.FileInfo, path, dir, pattern string) (*LogMeta, error) {
	var (
		fd  *os.File
		err error
	)
	stat, ok := logFile.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, fmt.Errorf("[genLogMeta] file stat failed, Not a syscall.Stat_t")
	}
	if fd, err = os.Open(path); err != nil {
		return nil, fmt.Errorf("[genLogMeta] open file, get file fd -> %w", err)
	}
	sign := make([]byte, SignContentSize)
	if _, err = fd.Read(sign); err != nil && !errors.Is(err, io.EOF) {
		// TODO(link.xk): put into poller, fd close?
		return nil, fmt.Errorf("[genLogMeta] read file to gen sign failed -> %w", err)
	}

	return &LogMeta{
		Dir:     dir,
		Pattern: pattern,
		path:    path,
		LogInfo: logFile,
		fMeta: &FileMeta{
			fd:    fd,
			Dev:   stat.Dev,
			Inode: stat.Ino,
			Hash:  md5.Sum(sign),
		},
	}, nil
}

// NOTICE: just register meta, do not send event
func (lw *LogWatcher) kRegisterAndWatchSingleFile(path string, logf *fs.FileInfo) error {
	meta, err := genLogMeta(*logf, path, lw.dir, lw.pattern)
	if err != nil {
		return fmt.Errorf("gen log meta %v -> %w", path, err)
	}

	if _, ok := lw.logMetas[path]; !ok {
		lw.logMetas[path] = meta
	}

	if err = lw.watcher.Add(path); err != nil {
		return fmt.Errorf("watcher add %v -> %w", path, err)
	}
	logger.Notice("[RegisterAndWatchSingleFile] file meta registerd", path)
	return nil
}

// RegisterAndWatch find files and register and watch them
func (lw *LogWatcher) RegisterAndWatch(dir, pattern string) error {
	lw.locker.Lock()
	defer lw.locker.Unlock()

	var (
		logFiles []fs.FileInfo
		pathList []string
		err      error
	)
	lw.dir, lw.pattern = dir, pattern

	if logFiles, pathList, err = findFiles(dir, pattern); err != nil {
		return fmt.Errorf("[LogWatcher] Register failed -> %w", err)
	}

	for i := range logFiles {
		logFile := logFiles[i]
		path := pathList[i]
		lw.kRegisterAndWatchSingleFile(path, &logFile)
	}

	for path := range lw.logMetas {
		defer lw.sendEvent(LogFileDiscover, lw.logMetas[path]) // NOTICE: Defer make sure inform uppper layer is in batch
	}
	return nil
}

func (lw *LogWatcher) Close() {
	logger.Debug("[LogWatcher] Close Start")
	defer logger.Debug("[LogWatcher] Close End")
	// clear up resouse
	// opened files, watcher, goroutines
	for _, v := range lw.logMetas {
		if v.fMeta.fd != nil {
			logger.Debug("[LogWatcher] File Close:", v.path, v.fMeta.Inode)
			v.fMeta.fd.Close()
		}
	}

	if lw.watcher != nil {
		lw.watcher.Close()
		logger.Debug("[LogWatcher] fsnitify watcher close")
	}

	if lw.closeC != nil {
		logger.Debug("[LogWatcher] handleEvent close start")
		lw.closeC <- struct{}{}
		logger.Debug("[LogWatcher] handleEvent close")
	}
	if lw.pollerCloseC != nil {
		lw.pollerCloseC <- struct{}{}
	}
}
