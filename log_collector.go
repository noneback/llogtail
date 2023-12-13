package llogtail

// import (
// 	"encoding/json"
// 	"errors"
// 	"fmt"
// 	"io"

// 	"sync/atomic"

// 	"log"
// 	"os"
// 	"sync"
// 	"syscall"
// 	"time"

// 	"github.com/eapache/queue"
// )

// var verbose = uint32(0)

// func Verbose(flag bool) {
// 	if flag {
// 		atomic.StoreUint32(&verbose, 1)
// 	} else {
// 		atomic.StoreUint32(&verbose, 0)
// 	}
// 	log.SetFlags(log.LstdFlags | log.Lshortfile)
// 	log.SetOutput(os.Stdout)
// }

// func init() {
// 	log.SetOutput(io.Discard)
// }

// const (
// 	kMaxLogReaderSize     = 10
// 	kCheckpointFileExt    = ".cpt"
// 	kOffsetDir            = "offset"
// 	kMaxLogCollectFailure = 5
// 	kMaxIdleRound         = 10
// )

// type LogConf struct {
// 	Dir     string `json:"dir"`
// 	Pattern string `json:"pattern"`
// 	LineSep string `json:"lineSeperator"`
// 	Sink    SinkConf `json:"sink"`
// }

// type SinkConf struct {
// 	Typ SinkType `json:"type"`
// 	Conf string `json:"config"`
// }

// type Checkpoint struct {
// 	Meta   FileMeta `json:"metadata"`
// 	Offset uint64   `json:"offset"`
// 	Name   string   `json:"name"`
// 	//TODO(noneback): readers string   `json:"readers"`
// }

// type TaskState uint64

// type LogReader struct {
// 	meta *LogMeta
// }

// type TaskContext struct {
// 	offset uint64
// 	meta   *LogMeta // note: it is a snapshot, rather than a real-time status
// 	delete bool     // if task is deleted
// 	isSet  bool     // if offset is setted
// }

// // LogCollector is a event-driven log file collector based on inotify
// // It mainly consists of a log watcher and a data collector.
// // Watcher watched registerd log file event, triggering data collector when necessary.
// //
// // LogCollector design in detail:
// // Mainly, LogCollector has two nonstopping independent goroutines, one is for Handling LogFileEvent, another is for Collectiong Data. Goroutines only exist when LogCollector stopped.
// //
// // 1. Handling LogFileEvent
// // LogWatcher is responsible for that, passing a LogFileEvent via a chan for upper layer. Collecting task is activated when a LogFileEvent comes.
// // LogFileEvent including Discover, Modify, Rename, Remove. Their handler varies.
// // - Discover: LogFileDiscover means that file is registerd first time during collector's lifetime. LogFile could be new found or an original one. For new log, we create a new cpt file and put msg to collect task queue. For a existed log, we read cpt file and validate metadata before put into queue. We should be aware that Discover Event determined the num of collection task, and Discover Event is only registerd by watcher.RegisterAndWatch Func.
// // - Modify: Most frequent-received event, just trigger collection,do nothing.
// // - Rename: Rename Event means that Log File has Retated. In that case, it generate a LogReader, and put reader into its reader queue, which will be used by collecting goroutine when last log file collection is finished.
// // - Remove: Mark corresponding collection task is deleted. Remove Event means that registerd log in certain path has beed deleted due to some mistake.
// //
// // 2. Collectiong Data
// // We use a Queue as Collection Task Queue. Each Task consists of necessary collection task metadata, such as log path, offset, if offset is set, if file is deleted. Collecting Data will be blocked if it meets too much idle round or buffer is full, and will be resumed when event come or buffer has room. And tasks flow in cycle in tasks queue. Always fetch head task and put back to tail when finished.
// //
// // For a normal task exection:
// //
// // Step 1. If Task is Deleted. If a task deleted, then we trying to fetch a reader from corresponding reader queue, and reconstruc the task using new metadata.
// //
// // Step 2. If Log File Offset Set. If not set(due to previous error or offset rollback), set offset.
// //
// // Step 3. Do Collection. Try to collect data from file, and data is maintained in a thread-safe buffer. EOF error occurs when collecting. And if EOF is caused by LogFileRotate, we fetch new reader and reconstruct the task.If EOF is caused by Truncate, we just reset its offset. If collection task has no progress, meaning that do collection finished with no data collected or error occurence, we increase the no progress counter, and blocking task queue when counter meets maxIdleRound.
// //
// // Step 4. Handle Cpt. Do Collection only change tasks metadata in memory, we need to persist it before we pass data to backend.
// type LogCollector struct {
// 	tasks         map[string]*TaskContext // path -> taskCtx
// 	readers       map[string]*queue.Queue // path -> readerQ, *LogReader, pending new log file after file rotate. dir + pattern -> Queue. NOTE: readers may lost if it crushs.
// 	watcher       LogWatcher              // watcher for registerd log patterns, watch log file rotate and discover,etc event.
// 	buf           *Buffer
// 	running       bool
// 	closeC        chan struct{}
// 	cpts          map[string]*Checkpoint // path -> cpt
// 	taskQ         *queue.Queue           // *TaskContext
// 	conf          *LogConf               // dir + pattern, etc...
// 	failedCounter map[string]int         // path -> num
// 	idleCond      *sync.Cond
// 	fromLastest   bool
// 	sink Sink
// }

// type LogCollectorOption struct {
// 	FilterInterval int  // in sec
// 	BufferSize     int  // in byte
// 	FromLastest    bool // collected from lastest offset
// }

// // TODO(link.xk): add some option: filter interval, buffer size
// func NewLogCollector(opt *LogCollectorOption) *LogCollector {
// 	bufferSize := DefaultLogBufferSize
// 	filterInterval := 30
// 	fromLastest := false
// 	if opt != nil {
// 		fromLastest = opt.FromLastest
// 		if opt.BufferSize > 0 {
// 			bufferSize = opt.BufferSize
// 		}
// 		if opt.FilterInterval > 0 {
// 			filterInterval = 10
// 			if opt.FilterInterval > 10 {
// 				filterInterval = opt.FilterInterval
// 			}
// 		}
// 	}

// 	return &LogCollector{
// 		readers: make(map[string]*queue.Queue),
// 		watcher: *NewLogWatcher(&LogWatchOption{
// 			FilterInterval: time.Second * time.Duration(filterInterval),
// 			PollerInterval: time.Minute * 5,
// 		}),
// 		buf:           NewBlockingBuffer(bufferSize),
// 		cpts:          make(map[string]*Checkpoint),
// 		running:       false,
// 		taskQ:         queue.New(),
// 		closeC:        make(chan struct{}),
// 		failedCounter: make(map[string]int),
// 		tasks:         make(map[string]*TaskContext),
// 		idleCond:      sync.NewCond(&sync.Mutex{}),
// 		fromLastest:   fromLastest,
// 	}
// }

// // Init plz make sure log is existed, otherwise init return a error and will not collect logs.
// // Once Log File is register, it cannot be changed or added. Log path registers only when it inits.
// func (lc *LogCollector) Init(conf *LogConf) error {
// 	if err := lc.watcher.Init(); err != nil {
// 		return fmt.Errorf("[LogCollector] watcher init -> %w", err)
// 	}
// 	lc.conf = conf

// 	switch lc.conf.Sink.Typ {
// 	case FileSinkType:
// 		lc.sink = NewFileSink()
// 	default:
// 		return fmt.Errorf("[LogCollector] sink %v not support",lc.conf.Sink )
// 	}

// 	logger.Notice("LogCollector init success")
// 	return nil
// }

// func (lc *LogCollector) Run() error {
// 	go lc.runBackground()
// 	if err := lc.watcher.RegisterAndWatch(lc.conf.Dir, lc.conf.Pattern); err != nil {
// 		lc.Close()
// 		return fmt.Errorf("[LogCollector] Init RegisterAndWatch -> %w", err)
// 	}
// 	for _, path := range lc.watcher.pathMap {
// 		lc.readers[path] = queue.New()
// 	}

// 	return nil
// }

// func (lc *LogCollector) runBackground() {
// 	lc.watcher.RunEventHandler() // start watcher event handler routine
// 	lc.running = true
// 	go lc.listenEvent()
// 	go lc.collectSchedule()
// }

// func (lc *LogCollector) listenEvent() {
// 	ticker := time.NewTicker(3 * time.Minute) // time unit can be slower
// 	defer ticker.Stop()

// 	for {
// 		select {
// 		case e := <-lc.watcher.EventC:
// 			lc.idleCond.Broadcast() // make cond notified
// 			if err := lc.handleEvent(e); err != nil {
// 				log.Printf("[LogCollector] listenEvent, err %v", err.Error())
// 			}
// 		case <-ticker.C:
// 			// avoid log collect no progress lockdown to long
// 			// in some cases, rename and idle lock happends in a random sequence, so there are chance
// 			// idleCond to be locked for a long time.
// 			log.Println("idleCond broadcast itself after 3 mins")
// 			lc.idleCond.Broadcast() // make cond notified
// 		case <-lc.closeC: // only trigger by Close
// 			lc.running = false
// 			log.Println("listenEvent Close")
// 			return
// 		}
// 	}
// }

// // read checkpoint and store it in cpts
// // TODO: add reader queue into checkpoint
// func (lc *LogCollector) readCheckpoint(path string) (*Checkpoint, error) {
// 	content, err := os.ReadFile(path)
// 	if err != nil {
// 		return nil, fmt.Errorf("readCheckpoint ReadFile %v -> %w", path, err)
// 	}
// 	cpt := &Checkpoint{}
// 	if err := json.Unmarshal(content, cpt); err != nil {
// 		return nil, fmt.Errorf("readCheckpoint Unmarshal %v -> %w", path, err)
// 	}
// 	lc.cpts[path] = cpt
// 	return cpt, nil
// }

// // makeCheckpoint gen checkpoint file or read a original one
// // and also responsiable cpts in LogCollector
// // TODO: put reader queue into checkpoint
// func (lc *LogCollector) makeCheckpoint(path string, meta *FileMeta, offset uint64) error {
// 	file, err := os.Create(path) // create a new one or truncate file
// 	cpt := &Checkpoint{*meta, uint64(offset), meta.fd.Name()}
// 	if err != nil {
// 		return fmt.Errorf("makeCheckpoint create or open checkpoint file %v -> %w", path, err)
// 	}
// 	defer file.Close()
// 	content, err := json.Marshal(cpt)
// 	if err != nil {
// 		return fmt.Errorf("makeCheckpoint Marshal -> %w", err)
// 	}
// 	// create a new file
// 	if err := os.WriteFile(path, content, os.ModeAppend); err != nil {
// 		return fmt.Errorf("makeCheckpoint write cpt file %v -> %w", path, err)
// 	}
// 	lc.cpts[path] = cpt
// 	// log.Println("makeCheckpoint success, path %v, file %v",path, meta.fd.Name())
// 	return nil
// }

// func (lc *LogCollector) handleEvent(event *Event) error {
// 	path := event.meta.path
// 	cptPath := genCptPath(path)
// 	switch event.e {
// 	case LogFileDiscover:
// 		// discover only trigger by RegisterAndWatch func.
// 		// LogFileDiscover means that file is register first time during collector's lifetime. LogFile could be new found or an original one.
// 		// For new log, we create a new cpt file and put msg to task queue. For a existed log, we read cpt file and validate metadata before put into queue.
// 		log.Printf("DiscoverFile %v, inode %v", event.meta.fMeta.Inode, event.meta.fMeta.fd.Name())
// 		task := &TaskContext{0, event.meta, false, false}
// 		lc.tasks[path] = task
// 		defer lc.taskQ.Add(task) // a discover has a task

// 		cpt, err := lc.readCheckpoint(cptPath)
// 		if err != nil && !errors.Is(err, os.ErrNotExist) {
// 			return fmt.Errorf("handleEvent read checkpoint -> %w", err)
// 		}
// 		// read cpt success, validate it.
// 		if err == nil {
// 			if validateCpt(cpt, event.meta) {
// 				task.offset = cpt.Offset
// 				if lc.fromLastest {
// 					task.offset = uint64(event.meta.LogInfo.Size())
// 				}
// 				return nil
// 			}
// 		}
// 		// if validate is not pass, or cpt file not exists, create new one
// 		if err := lc.makeCheckpoint(cptPath, event.meta.fMeta, 0); err != nil {
// 			return fmt.Errorf("handleEvent make checkpoint -> %w", err)
// 		}
// 		task.isSet = true
// 	case LogFileRenameRotate:
// 		// Rotate Trigger by LogWatcher when log rorates.
// 		// Those logMeta have been registered. Just put msg into readers
// 		if !lc.isInCollector(event.meta) {
// 			// filter dup meta from lw.poller
// 			log.Printf("RenameRotate: add a reader, inode %v, filepath %v\n", event.meta.fMeta.Inode, event.meta.path)
// 			lc.readers[path].Add(&LogReader{event.meta})
// 		} else {
// 			log.Printf("Rename event come, but already in readerQ or tasks, file path %v\n", event.meta.path)
// 		}
// 	case LogFileRemove:
// 		// Removed Event trigger when file is removed.
// 		log.Printf("Remove LogFile %v\n", event.meta.path)
// 		lc.tasks[event.meta.path].delete = true // TODO(link.xk): need to check
// 	case LogFileModify:
// 		log.Printf("Modify LogFile %v\n", event.meta.path)
// 	}
// 	return nil
// }

// // return true if meta is in readerQ or tasks
// func (lc *LogCollector) isInCollector(meta *LogMeta) bool {
// 	readerQ := lc.readers[meta.path]
// 	// is file in readerQ
// 	for i := 0; i < readerQ.Length(); i++ {
// 		lr := readerQ.Get(i).(*LogReader)
// 		if lr.meta.path == meta.path && lr.meta.fMeta.Dev == meta.fMeta.Dev && lr.meta.fMeta.Inode == meta.fMeta.Inode {
// 			return true
// 		}
// 	}
// 	// is file in task
// 	for _, task := range lc.tasks {
// 		if task.meta.path == meta.path && task.meta.fMeta.Dev == meta.fMeta.Dev && task.meta.fMeta.Inode == meta.fMeta.Inode { // TODO: add sign
// 			return true
// 		}
// 	}

// 	return false
// }

// func (lc *LogCollector) isTruncate(taskCtx *TaskContext) bool {
// 	cptPath := genCptPath(taskCtx.meta.path)
// 	cpt := lc.cpts[cptPath]
// 	cptOffset := lc.cpts[cptPath].Offset

// 	file, err := os.Stat(taskCtx.meta.path)
// 	if err != nil {
// 		log.Printf("isTruncate Open file failed, consider it a trucate, path %v, err %v\n", taskCtx.meta.path, err.Error())
// 		// taskCtx.delete = true
// 		return false
// 	}
// 	stat, _ := file.Sys().(*syscall.Stat_t)
// 	if stat.Dev == cpt.Meta.Dev && stat.Ino == cpt.Meta.Inode {
// 		if file.Size() < int64(cptOffset) {
// 			return true
// 		}
// 	}
// 	return false
// }

// // collectOnce focus on collect and update in memory metadata
// // return false when collect has no progress(no data collected, no error, EOF)
// // if we meet a error besides EOF, we consider it having progress
// func (lc *LogCollector) collectOnce(taskCtx *TaskContext) (bool, error) {
// 	defer ExectionTimeCost("CollectOnce:"+taskCtx.meta.path, time.Now())
// 	path := taskCtx.meta.path
// 	cptPath := genCptPath(path)
// 	cpt := lc.cpts[cptPath] // no need to check

// 	// if task is mark as deleted, we need to clear it and load a new task from readerQ if it is not empty.
// 	if lc.isTaskDeleted(taskCtx) {
// 		// note: A deleted task fd should already be released
// 		log.Printf("Found a Deleted Task, file %v\n", path)
// 		readerQ := lc.readers[path]
// 		if readerQ.Length() <= 0 {
// 			log.Printf("collectOnce readerQ and taskCtx is empty, info %v\n", taskCtx.meta.path)
// 			return false, nil
// 		}
// 		reader := readerQ.Remove().(*LogReader)
// 		taskCtx.meta = reader.meta
// 		taskCtx.offset = 0
// 		taskCtx.delete = false
// 		taskCtx.isSet = false
// 		cpt.Offset = 0
// 	}

// 	// task is valid but not set, set offset
// 	if !lc.isTaskSet(taskCtx) {
// 		if _, err := taskCtx.meta.fMeta.fd.Seek(int64(cpt.Offset), io.SeekStart); err != nil {
// 			return true, fmt.Errorf("collectOnce file seek when task is not set -> %w", err)
// 		}
// 		taskCtx.isSet = true
// 	}

// 	// task must be valid and set, do collect, fill the buffer
// 	fd := taskCtx.meta.fMeta.fd
// 	n, err := lc.buf.ReadLinesFrom(fd, lc.conf.LineSep)
// 	if err != nil {
// 		if !errors.Is(err, io.EOF) && !errors.Is(err, ErrNoProgress) {
// 			return true, fmt.Errorf("collectOnce read closed file to buffer -> %w", err)
// 		}
// 		// if we meet EOF, log may have been rotated or log is not generated or handle it later on.
// 		// if file is rename rotate, we pull a read into taskQ.
// 		if lc.isTaskDone(taskCtx) {
// 			log.Printf("Log File %v Collection finished, n %v\n", taskCtx.meta.path, n)
// 			taskCtx.meta.fMeta.fd.Close() // TODO(link.xk): check, fd close only in here
// 			// NOTE: checkpoint, taskCtx should be updated
// 			reader := lc.readers[path].Remove().(*LogReader)
// 			taskCtx.meta = reader.meta
// 			taskCtx.offset = 0
// 			cpt.Offset = 0
// 			taskCtx.isSet = false
// 			return true, nil
// 		}
// 		// task is not done, maybe file truncate or removed-created
// 		if lc.isTruncate(taskCtx) {
// 			log.Printf("CollectOnce file %v is Truncate\n", taskCtx.meta.path)
// 			cpt.Offset = 0
// 			taskCtx.offset = 0
// 			taskCtx.isSet = false
// 			return true, nil
// 		}
// 		// EOF or no prgress, but task is not over
// 		return false, nil
// 	}
// 	// gen file hash
// 	taskCtx.isSet = false // make sure next read from a new line.
// 	if n != 0 && taskCtx.offset <= SignContentSize {
// 		if _, err := fd.Seek(int64(0), io.SeekStart); err != nil {
// 			return true, fmt.Errorf("collectOnce file seek 0 to gen hash -> %w", err)
// 		}
// 		hash, err := genFileSign(fd)
// 		if err != nil {
// 			return true, fmt.Errorf("collectOnce genFileSign -> %w", err)
// 		}
// 		cpt.Meta.Hash = *hash
// 		taskCtx.meta.fMeta.Hash = *hash
// 		if _, err := fd.Seek(int64(taskCtx.offset)+int64(n), io.SeekStart); err != nil {
// 			return true, fmt.Errorf("collectOnce file seek %v to resume  -> %w", taskCtx.offset, err)
// 		}
// 	}
// 	taskCtx.offset += uint64(n)
// 	cpt.Offset += uint64(n)
// 	// Normally, if code running at this line, n != 0, but let's double check anyway.
// 	return n != 0, nil
// }

// // GetCollectedData return data and reset buf
// func (lc *LogCollector) GetCollectedData() []byte {
// 	return lc.buf.Fetch()
// }

// func (lc *LogCollector) collectSchedule() {
// 	noProgress := 0
// 	round := 10
// 	for lc.running {
// 		// try to slow it down
// 		if lc.taskQ.Length() == 0 {
// 			log.Println("[collectSchedule] taskQ idle, sleep 1s")
// 			time.Sleep(1 * time.Second)
// 			continue
// 		}

// 		taskCtx := lc.taskQ.Remove().(*TaskContext)
// 		path := taskCtx.meta.path
// 		cptPath := genCptPath(path)
// 		cpt := lc.cpts[cptPath] // no need to check, require make sure it exists.

// 		hasProgress, err := lc.collectOnce(taskCtx)
// 		// TODO(link.xk): check, do we really need it?
// 		if err != nil {
// 			log.Printf("[LogCollector] collectSchedule failed %v\n", err.Error())
// 			lc.failedCounter[path]++
// 			// TODO(link.xk): check
// 			if lc.failedCounter[path] >= kMaxLogCollectFailure {
// 				log.Printf("[LogCollector] collectorSchedule task fail constantly, cpt %v\n", cptPath)
// 				taskCtx.delete = true
// 				taskCtx.meta.fMeta.fd.Close() // TODO(link.xk): check
// 			}
// 		} else {
// 			lc.failedCounter[path] = 0
// 		}

// 		if err := lc.makeCheckpoint(cptPath, taskCtx.meta.fMeta, cpt.Offset); err != nil {
// 			log.Printf("[LogCollector] collectorSchedule makeCheckpoint, cpt %v, err %v\n", cptPath, err.Error())
// 		}

// 		lc.taskQ.Add(taskCtx)

// 		if hasProgress {
// 			noProgress = 0
// 		} else {
// 			noProgress++
// 		}
// 		if noProgress >= round {
// 			lc.idleCond.L.Lock()
// 			logger.Debugf("meet idle Cond, taskQ waitting, file %v\n", taskCtx.meta.path)
// 			lc.idleCond.Wait()
// 			logger.Debugf("event comes, taskQ signaledfile %v\n", taskCtx.meta.path)
// 			noProgress = 0
// 			lc.idleCond.L.Unlock()
// 		}
// 		// when buf is not full, we may still need data fetched
// 		// when data is full, it block itself
// 		round = len(lc.tasks) // it will stablized when all file is found
// 		logger.Debugf("IfFullThenWait start %v", taskCtx.meta.path)
// 		lc.buf.IfFullThenWait()
// 		logger.Debugf("IfFullThenWait end %v", taskCtx.meta.path)
// 	}
// }

// // isTaskDone return ture if task corresponding reader queue is not empty
// func (lc *LogCollector) isTaskDone(task *TaskContext) bool {
// 	return lc.readers[task.meta.path].Length() != 0
// }

// // task can be delete for log truncate or failure
// func (lc *LogCollector) isTaskDeleted(task *TaskContext) bool {
// 	return task.delete
// }

// // task is set when its file offset is the same as cpt.offset
// func (lc *LogCollector) isTaskSet(task *TaskContext) bool {
// 	return task.isSet
// }

// // TODO(noneback): need change
// func (lc *LogCollector) Collect() ([]byte, error) {
// 	// log.Debug("[LogCollector] call Collect", log.String("file", lc.conf.Pattern))
// 	return lc.GetCollectedData(), nil
// }

// // TODO(link.xk): make sure all resource has been released
// func (lc *LogCollector) Close() error {
// 	log.Println("LogCollector begin Close")
// 	defer log.Println("LogCollector finish Close")
// 	for lc.taskQ.Length() != 0 {
// 		fd := lc.taskQ.Remove().(*TaskContext).meta.fMeta.fd
// 		if fd != nil {
// 			fd.Close()
// 		}
// 	}

// 	for _, r := range lc.readers {
// 		for r.Length() != 0 {
// 			fd := r.Remove().(*LogMeta).fMeta.fd
// 			if fd != nil {
// 				fd.Close()
// 			}
// 		}
// 	}

// 	lc.watcher.Close() // make sure watcher is closed before listenEvent close.
// 	// fire collect goroutine to exit
// 	lc.closeC <- struct{}{}
// 	lc.idleCond.Broadcast()
// 	time.Sleep(200 * time.Millisecond)
// 	lc.buf.cond.Broadcast()
// 	lc.buf.Close()
// 	lc.buf = nil
// 	return nil
// }
