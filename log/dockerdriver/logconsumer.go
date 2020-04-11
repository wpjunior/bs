package dockerdriver

import (
	"encoding/binary"
	"io"
	"log"
	"sync"

	"github.com/docker/docker/api/types/plugins/logdriver"
	protoio "github.com/gogo/protobuf/io"
)

const MAX_LINES_BY_CONTAINER = 100

// TODO: add prometheus metrics
// gauge of number of activestreams
// counter of lines of log

type LogConsumer interface {
	StartConsume(containerID string, buf io.ReadCloser)
	ReadLogs(containerID string) *logReader
}

type ringEntry struct {
	entry      logdriver.LogEntry
	prev, next *ringEntry
}

type logBuffer struct {
	mu   sync.Mutex
	ring *ringEntry
}

type logConsumer struct {
	mu         sync.Mutex
	containers map[string]string
	buffers    map[string]*logBuffer
}

type logReader struct {
	buffer *logBuffer
	C      chan logdriver.LogEntry
}

func (l *logReader) startRead() {
	l.buffer.mu.Lock()
	defer l.buffer.mu.Unlock()

	current := l.buffer.ring.next
	for {
		if len(current.entry.Line) == 0 {
			current = current.next
			continue
		}
		l.C <- current.entry

		current = current.next
		if current == l.buffer.ring {
			break
		}
	}

	close(l.C)

}

func (l *logConsumer) ReadLogs(containerID string) *logReader {

	l.mu.Lock()
	buffer := l.buffers[containerID]
	l.mu.Unlock()

	if buffer == nil {
		return nil
	}

	reader := &logReader{
		buffer: buffer,
		C:      make(chan logdriver.LogEntry, 100),
	}
	go reader.startRead()

	return reader
}

func (l *logConsumer) StartConsume(containerID string, buf io.ReadCloser) {
	l.mu.Lock()
	buffer := newLogBuffer()
	l.buffers[containerID] = buffer
	l.mu.Unlock()

	defer buf.Close()
	dec := protoio.NewUint32DelimitedReader(buf, binary.BigEndian, 1e6)
	defer dec.Close()

	var logEntry logdriver.LogEntry
	for {
		logEntry.Reset()
		err := dec.ReadMsg(&logEntry)
		if err == io.EOF {
			return
		} else if err != nil {
			log.Printf("Failed to consume log, container ID: %s, error: %s", containerID, err.Error())
			return
		}

		buffer.mu.Lock()
		buffer.ring.entry = logEntry
		buffer.ring = buffer.ring.next
		buffer.mu.Unlock()
	}
}

func newLogConsumer() LogConsumer {
	return &logConsumer{
		mu:         sync.Mutex{},
		containers: map[string]string{},
		buffers:    map[string]*logBuffer{},
	}
}

func newLogBuffer() *logBuffer {
	start := &ringEntry{}
	next := start

	for i := 0; i < MAX_LINES_BY_CONTAINER; i++ {
		next.next = &ringEntry{prev: next}
		next = next.next
	}

	next.next = start
	start.prev = next

	return &logBuffer{ring: next}
}
