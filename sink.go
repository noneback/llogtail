package llogtail

type Sink interface {
	Open(string) error
	Push(fpath string, content []byte) error
	Close() error
}

type SinkType string

const (
	FileSinkType SinkType = "file"
)
