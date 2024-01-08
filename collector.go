package llogtail

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/eapache/queue"
)

var (
	ErrNoProgress = errors.New("Buffer Read From Line, No Prgress")
)

const kDefaultLineSep = "\n"

// single file collector handle
type kCollector struct {
	fpath    string        // file path
	task     *kTaskContext // current collect task
	waitting *queue.Queue  // wait to be collect, *LogReader
	buf      *BlockingBuffer
	cpt      *kCheckpoint
	joining  bool
}

type kTaskContext struct {
	offset uint64
	meta   *LogMeta // Note: it is a snapshot, rather than a real-time status
}

func newCollector(meta *LogMeta) *kCollector {
	return &kCollector{
		fpath:    meta.path,
		waitting: queue.New(),
		buf:      NewBlockingBuffer(DefaultLogBufferSize),
		task: &kTaskContext{
			offset: 0,
			meta:   meta,
		},
		cpt:     nil,
		joining: false,
	}
}

func (c *kCollector) init() error {
	cptPath := genCptPath(c.fpath)

	cpt, err := readCheckpoint(cptPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read checkpoint -> %w", err)
	}
	// read cpt success, validate it.
	if err == nil {
		if validateCpt(cpt, c.task.meta) {
			c.cpt = cpt
		}
	}

	// if validate is not pass, or cpt file not exists, create new one
	c.cpt, err = makeCheckpoint(cptPath, c.task.meta.fMeta, 0)
	if err != nil {
		return fmt.Errorf("make checkpoint -> %w", err)
	}
	return nil
}

func (c *kCollector) fetch() ([]byte, error) {
	// logger.Debugf("IfFullThenWait start %v", c.fpath)
	// c.buf.IfFullThenWait() // TODO(noneback): check
	// logger.Debugf("IfFullThenWait end %v", c.fpath)
	fd := c.task.meta.fMeta.fd
	n, err := c.buf.ReadLinesFrom(fd, kDefaultLineSep)
	if err != nil {
		if !errors.Is(err, io.EOF) && errors.Is(err, ErrNoProgress) {
			return nil, fmt.Errorf("read closed file to buffer -> %w", err)
		}
		// EOF
		if c.waitting.Length() != 0 {
			c.roll()
			return nil, nil
		}
	}
	// no err, collect file success
	if n != 0 && c.task.offset <= SignContentSize {
		// if file size if less than  SignContentSize, we need to gen sign until it fill it.
		if _, err := fd.Seek(int64(0), io.SeekStart); err != nil {
			return nil, fmt.Errorf("file seek 0 to gen hash -> %w", err)
		}
		hash, err := genFileSign(fd)
		if err != nil {
			return nil, fmt.Errorf("genFileSign -> %w", err)
		}
		c.cpt.Meta.Hash = *hash
		c.task.meta.fMeta.Hash = *hash
		if _, err := fd.Seek(int64(c.task.offset)+int64(n), io.SeekStart); err != nil {
			return nil, fmt.Errorf("file seek %v to resume after gen sign -> %w", c.task.offset+uint64(n), err)
		}
	}

	c.task.offset += uint64(n)
	c.cpt.Offset += uint64(n)
	//  TODO(noneback): check
	return c.buf.Fetch(), nil
}

func (c *kCollector) push(meta *LogMeta) {
	if !c.joining && !c.contain(meta) {
		c.waitting.Add(meta)
	}
}

func (c *kCollector) stop() {
	if c.task.meta.fMeta.fd != nil {
		c.task.meta.fMeta.fd.Close()
	}

	for c.waitting.Length() != 0 {
		fd := c.waitting.Remove().(*LogMeta).fMeta.fd
		if fd != nil {
			fd.Close()
		}
	}

	c.buf.cond.Broadcast()
	c.buf.Close()
	c.buf = nil
}

func (c *kCollector) roll() {
	meta := c.waitting.Remove().(*LogMeta)
	c.task.meta = meta
	c.task.offset = 0
	c.cpt.Meta = *meta.fMeta
	c.cpt.Offset = 0
}

func (c *kCollector) join() {
	c.joining = true
}

func (c *kCollector) contain(meta *LogMeta) bool {
	// is file in readerQ
	for i := 0; i < c.waitting.Length(); i++ {
		lr := c.waitting.Get(i).(*LogReader)
		if lr.meta.path == meta.path && lr.meta.fMeta.Dev == meta.fMeta.Dev && lr.meta.fMeta.Inode == meta.fMeta.Inode {
			return true
		}
	}
	// is file in task
	if c.task.meta.path == meta.path && c.task.meta.fMeta.Dev == meta.fMeta.Dev && c.task.meta.fMeta.Inode == meta.fMeta.Inode { // TODO: add sign
		return true
	}
	return false
}

func (c *kCollector) sink() error {
	return nil
}
