package llogtail

type Sink interface {
	Open(string) error
	Push([]byte) error
	Close() error
}

type SinkType string

const (
	FileSinkType SinkType = "file"
)

type FileSink struct {
	Sink
	dst string
}


func NewFileSink() Sink {
	return &FileSink{
	}
}

func (s *FileSink) Open(conf string)error {
	return nil
}

func (s *FileSink) Push(content []byte) error{
	return nil
}

func (s *FileSink) Close()error{
	return nil
}