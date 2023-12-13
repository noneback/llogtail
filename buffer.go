package llogtail

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"sync"
)

type kDataUnit struct {
	File    string `json:"file"`
	Content []byte `json:"content"`
}

func encodeDataUnit(ctx *kTaskContext, content []byte) ([]byte, error) {
	raw, err := json.Marshal(
		&kDataUnit{
			File:    ctx.meta.path,
			Content: content,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("marshal data unit -> %w", err)
	}
	return raw, nil
}

const (
	DefaultLogBufferSize = MB + 1048
	MaxBufferSize        = 4 * MB
	MB                   = 1024 * 1024
	KB                   = 1024
)

var (
	ErrNoProgress = errors.New("Buffer Read From Line, No Prgress")
)

// BlockingBuffer is a thread-safe buffer, with blocking api.
// It block itself when data is full
type BlockingBuffer struct {
	data       []byte
	readBuffer []byte
	offset     int
	cap        int
	full       bool
	cond       *sync.Cond
	init       bool
}

func NewBlockingBuffer(bufferSize int) *BlockingBuffer {
	return &BlockingBuffer{
		offset: 0,
		cap:    bufferSize,
		full:   false,
		cond:   sync.NewCond(&sync.Mutex{}),
		init:   false,
	}
}

func (b *BlockingBuffer) reset() {
	b.offset = 0
}

// Fetch fetch data in buf and notify Waited Write
func (b *BlockingBuffer) Fetch() []byte {
	defer b.cond.Broadcast()
	b.cond.L.Lock()
	defer b.cond.L.Unlock()
	offset := b.offset
	copy(b.readBuffer, b.data[:b.offset])
	b.reset()
	b.full = false
	return b.readBuffer[:offset]
}

func (b *BlockingBuffer) IfFullThenWait() {
	b.cond.L.Lock()
	defer b.cond.L.Unlock()

	for b.full {
		log.Println("[buffer] buffer is full, wait")
		b.cond.Wait()
	}
}

// ReadLinesFrom make sure buffer read at least a line or none.
func (b *BlockingBuffer) ReadLinesFrom(reader *os.File, lineSep string) (int, error) {
	b.cond.L.Lock()
	defer b.cond.L.Unlock()
	// lazy loaded
	if !b.init {
		b.data = make([]byte, b.cap)
		b.readBuffer = make([]byte, b.cap)
	}

	b.init = true
	n, err := reader.Read(b.data[b.offset:])
	if err != nil {
		return n, err // if err = EOF, n will be 0, so just return
	}
	// EOF or read until end
	actualOffset := n + b.offset
	idx := bytes.LastIndex(b.data[b.offset:actualOffset], []byte(lineSep))
	n = idx + 1 // if idx == -1, then n = 0, else n = idx + 1
	b.offset += n
	if actualOffset >= b.cap {
		b.full = true
		b.enlarge()
	}
	if n == 0 {
		log.Printf("[buffer] ReadLine Maybe no progress\n")
		return 0, ErrNoProgress // make it an eof to trigger
	}
	return n, nil
}

// // ReadFrom mainly for maintain offset and enlarge, do not deal with error
// func (b *Buffer) ReadFrom(reader *os.File) (int, error) {
// 	b.cond.L.Lock()
// 	defer b.cond.L.Unlock()

// 	n, err := reader.Read(b.data[b.offset:]) // todo end with sep
// 	if err != nil {
// 		return n, err // if err = EOF, n will be 0, so just return
// 	}
// 	// EOF or read until end
// 	b.offset += n
// 	if b.offset >= b.cap {
// 		b.full = true
// 		b.enlarge()
// 	}
// 	return n, nil
// }

// simple way, thread safe
func (b *BlockingBuffer) enlarge() {
	old, new := len(b.data), len(b.data)*2
	if old >= MaxBufferSize-1 {
		return
	}

	if old > MB && old <= MaxBufferSize {
		new = int(math.Floor(float64(old) * 1.25))
	}
	if new >= MaxBufferSize {
		new = MaxBufferSize - 1
	}
	newBuf := make([]byte, new)
	copy(newBuf, b.data)
	b.data = newBuf
	b.readBuffer = make([]byte, new)
	b.cap = new
}

func (b *BlockingBuffer) Close() {
	b.data = nil
	b.readBuffer = nil
	b.init = false
}
