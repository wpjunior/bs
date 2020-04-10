package dockerdriver

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/docker/docker/api/types/plugins/logdriver"
	protoio "github.com/gogo/protobuf/io"
)

const MAX_LINES_BY_CONTAINER = 100

// TODO: add prometheus metrics
// gauge of number of activestreams
// counter of lines of log

type LogConsumer interface {
	StartConsume(containerID, filePath string, buf io.ReadWriteCloser)
	ReadLogs(containerID string) []logdriver.LogEntry
}

type ringEntry struct {
	entry      logdriver.LogEntry
	next, prev *ringEntry
}

type logBuffer struct {
	start, end *ringEntry
}

type logConsumer struct {
	mu         sync.Mutex
	containers map[string]string
	buffers    map[string]*logBuffer
}

func (l *logConsumer) ReadLogs(containerID string) []logdriver.LogEntry {
	logs := make([]logdriver.LogEntry, MAX_LINES_BY_CONTAINER)

	l.mu.Lock()
	buffer := l.buffers[containerID]
	l.mu.Unlock()

	if buffer == nil {
		return nil
	}
	count := 0
	for current := buffer.end; count < MAX_LINES_BY_CONTAINER; {
		logs[count] = current.entry
		count++

		current = current.prev
		if current == buffer.end {
			break
		}
	}
	return logs
}

func (l *logConsumer) StartConsume(containerID, filePath string, buf io.ReadWriteCloser) {
	l.mu.Lock()
	l.containers[containerID] = filePath
	buffer := newLogBuffer()
	l.buffers[containerID] = buffer
	l.mu.Unlock()

	defer buf.Close()
	dec := protoio.NewUint32DelimitedReader(buf, binary.BigEndian, 1e6)
	defer dec.Close()

	var logEntry logdriver.LogEntry
	for {
		err := dec.ReadMsg(&logEntry)
		if err == io.EOF {
			return
		}
		if err != nil {
			fmt.Println(err)
		}
		buffer.end.entry = logEntry
		buffer.end = buffer.end.next
		buffer.start = buffer.start.next

		logEntry.Reset()
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

	for i := 1; i < MAX_LINES_BY_CONTAINER; i++ {
		next.next = &ringEntry{prev: next}
		next = next.next
	}

	next.next = start
	start.prev = next

	return &logBuffer{start: next, end: start}
}
