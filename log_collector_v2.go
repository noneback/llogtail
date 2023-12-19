package llogtail

import (
	"fmt"

	"log"
	"time"
)

var verbose = uint32(0)

const (
	kMaxLogReaderSize     = 10
	kCheckpointFileExt    = ".cpt"
	kOffsetDir            = "offset"
	kMaxLogCollectFailure = 5
	kMaxIdleRound         = 10
)

type LogConf struct {
	Dir     string           `json:"dir"`
	Pattern string           `json:"pattern"`
	LineSep string           `json:"lineSeperator"`
	Sink    SinkConf         `json:"sink"`
	Watcher LogWatcherOption `json:"watcher"`
}

type SinkConf struct {
	Typ  SinkType `json:"type"`
	Conf string   `json:"config"`
}

type kCheckpoint struct {
	Meta   FileMeta `json:"metadata"`
	Offset uint64   `json:"offset"`
	Name   string   `json:"name"`
	//TODO(noneback): readers string   `json:"readers"`
}

type TaskState uint64

type LogReader struct {
	meta *LogMeta
}

type LogCollector struct {
	watcher    *LogWatcher            // watcher for registerd log patterns, watch log file rotate and discover,etc event.
	collectors map[string]*kCollector // file -> collector
	running    bool
	closeC     chan struct{}
	sink       Sink
	conf       LogConf
}

type LogWatcherOption struct {
	FilterInterval int `json:"filter"` // in sec
	PollerInterval int `json:"poller"` // in byte
}

// TODO(link.xk): add some option: filter interval, buffer size
func NewLogCollector() *LogCollector {
	return &LogCollector{
		running:    false,
		closeC:     make(chan struct{}),
		collectors: make(map[string]*kCollector),
	}
}

// Init plz make sure log is existed, otherwise init return a error and will not collect logs.
// Once Log File is register, it cannot be changed or added. Log path registers only when it inits.
func (lc *LogCollector) Init(conf LogConf) error {
	lc.conf = conf
	lc.watcher = NewLogWatcher(&LogWatchOption{
		time.Duration(conf.Watcher.FilterInterval) * time.Second, time.Duration(conf.Watcher.PollerInterval) * time.Second,
	})

	switch lc.conf.Sink.Typ {
	case FileSinkType:
		lc.sink = NewFileSink()
	default:
		return fmt.Errorf("[LogCollector] sink %v not support", lc.conf.Sink)
	}

	// init
	if err := lc.watcher.Init(); err != nil {
		return fmt.Errorf("[LogCollector] watcher init -> %w", err)
	}
	logger.Notice("LogCollector init success")
	return nil
}

func (lc *LogCollector) Run() error {
	lc.running = true
	go lc.runBackground()

	if err := lc.watcher.RegisterAndWatch(lc.conf.Dir, lc.conf.Pattern); err != nil {
		lc.Close()
		return fmt.Errorf("[LogCollector] Init RegisterAndWatch -> %w", err)
	}
	return nil
}

func (lc *LogCollector) runBackground() {
	lc.watcher.RunEventHandler() // start watcher event handler routine
	go lc.listenEvent()
}

func (lc *LogCollector) listenEvent() {
	ticker := time.NewTicker(3 * time.Minute) // time unit can be slower
	defer ticker.Stop()

	for {
		select {
		case e := <-lc.watcher.EventC:
			if err := lc.handleEvent(e); err != nil {
				log.Printf("[LogCollector] listenEvent, err %v", err.Error())
			}
		case <-ticker.C:
			// avoid log collect no progress lockdown to long
			// in some cases, rename and idle lock happends in a random sequence, so there are chance
			// idleCond to be locked for a long time.

		case <-lc.closeC: // only trigger by Close
			lc.running = false
			return
		}
	}
}

func (lc *LogCollector) handleEvent(event *Event) error {
	path := event.meta.path

	switch event.e {
	case LogFileDiscover:
		// discover only trigger by RegisterAndWatch func.
		// LogFileDiscover means that file is register first time during collector's lifetime. LogFile could be new found or an original one.
		// For new log, we create a new cpt file and put msg to task queue. For a existed log, we read cpt file and validate metadata before put into queue.
		logger.Noticef("DiscoverFile %v, inode %v", event.meta.fMeta.Inode, event.meta.fMeta.fd.Name())

		handle := newCollector(event.meta)
		if err := handle.init(); err != nil {
			return fmt.Errorf("collect init -> %w", err)
		}
		// TODO: fire it
		lc.collectors[path] = handle
	case LogFileRenameRotate:
		// Rotate Trigger by LogWatcher when log rorates.
		// Those logMeta have been registered. Just put msg into readers
		c := lc.collectors[event.meta.path] // NOTICE: cannot be missing
		if !c.contain(event.meta) {
			// filter dup meta from lw.poller
			logger.Noticef("RenameRotate: add a reader, inode %v, filepath %v\n", event.meta.fMeta.Inode, event.meta.path)
			c.push(event.meta)
		} else {
			logger.Infof("Rename event come, but already in readerQ or tasks, file path %v\n", event.meta.path)
		}
	case LogFileRemove:
		// Removed Event trigger when file is removed.
		logger.Noticef("Remove LogFile %v\n", event.meta.path)
		// TODO: delete collector
	case LogFileModify:
		// log.Printf("Modify LogFile %v\n", event.meta.path)
	}
	return nil
}

// TODO(link.xk): make sure all resource has been released
func (lc *LogCollector) Close() error {
	log.Println("LogCollector begin Close")
	defer log.Println("LogCollector finish Close")
	return nil
}
