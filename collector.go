package llogtail

import (
	"errors"
	"fmt"
	"os"

	"github.com/eapache/queue"
)

type kCollector struct {
	fpath    string        // file path
	task     *kTaskContext // current collect task
	waitting *queue.Queue  // wait to be collect, *LogReader
	buf      *BlockingBuffer
	cpt      *kCheckpoint
}

type kTaskContext struct {
	offset uint64
	meta   *LogMeta // Note: it is a snapshot, rather than a real-time status
	// delete bool     // if task is deleted
	// isSet  bool     // if offset is setted
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
		cpt: nil,
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
	if err := makeCheckpoint(cptPath, c.task.meta.fMeta, 0); err != nil {
		return fmt.Errorf("make checkpoint -> %w", err)
	}
	return nil
}

func (c *kCollector) push(meta *LogMeta) {
	c.waitting.Add(meta)
}
func (c *kCollector) stop()             {}

func (c *kCollector) join() {}


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