package llogtail

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/eapache/queue"
)

var (
	ErrNoProgress = errors.New("Buffer Read From Line, No Prgress")
)

const kDefaultLineSep = "\n"

// single file collector handle
type kCollector struct {
	workdir  string
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

func newCollector(workdir string, meta *LogMeta) *kCollector {
	return &kCollector{
		workdir:  workdir,
		fpath:    meta.path,
		waitting: queue.New(),
		buf:      NewBlockingBuffer(kDefaultLogBufferSize),
		task: &kTaskContext{
			offset: 0,
			meta:   meta,
		},
		cpt:     nil,
		joining: false,
	}
}

func (c *kCollector) init() error {
	cptPath := c.genCptPath(c.fpath)

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
	fd := c.task.meta.fMeta.fd
	// set offset
	if _, err := fd.Seek(int64(c.cpt.Offset), io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek offset %v to %v -> %w", fd.Name(), c.cpt.Offset, err)
	}

	n, err := c.buf.ReadLinesFrom(fd, kDefaultLineSep)
	if err != nil {
		if !errors.Is(err, io.EOF) && errors.Is(err, ErrNoProgress) {
			return nil, fmt.Errorf("read closed file to buffer -> %w", err)
		}
		// EOF
		if c.waitting.Length() != 0 {
			c.roll()
			logger.Noticef("kCollector roll")
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

func (c *kCollector) checkpoint() (err error) {
	cptPath := c.genCptPath(c.fpath)
	_, err = makeCheckpoint(cptPath, c.task.meta.fMeta, c.cpt.Offset)
	if err != nil {
		return fmt.Errorf("make checkpoint -> %w", err)
	}
	return nil
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
	// TODO: make sure data is collectted
}

func (c *kCollector) contain(meta *LogMeta) bool {
	// is file in readerQ
	for i := 0; i < c.waitting.Length(); i++ {
		lm := c.waitting.Get(i).(*LogMeta)
		if lm.path == meta.path && lm.fMeta.Dev == meta.fMeta.Dev && lm.fMeta.Inode == meta.fMeta.Inode {
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

// gen cpt path, path is log file path
func (c *kCollector) genCptPath(path string) string {
	hash := md5.Sum([]byte(path))
	checksum := []byte{hash[0], hash[1], hash[2], hash[3]}
	cptPath := filepath.Join(c.workdir, kOffsetDir, hex.EncodeToString(checksum)) + kCheckpointFileExt
	logger.Warning(cptPath, c.workdir)
	return cptPath
}
