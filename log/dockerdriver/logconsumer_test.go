package dockerdriver

import (
	"encoding/binary"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/docker/docker/api/types/plugins/logdriver"
	protoio "github.com/gogo/protobuf/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStartConsume(t *testing.T) {
	logConsumer := newLogConsumer()

	r, w := io.Pipe()
	go logConsumer.StartConsume("mycontainer", r)

	enc := protoio.NewUint32DelimitedWriter(w, binary.BigEndian)

	for i := 0; i < 10; i++ {
		err := enc.WriteMsg(&logdriver.LogEntry{
			Source:   "stdout",
			Line:     []byte(fmt.Sprintf("oi-%d", i)),
			TimeNano: time.Now().UnixNano(),
		})
		require.NoError(t, err)
	}

	time.Sleep(time.Second)
	reader := logConsumer.ReadLogs("mycontainer")

	i := 0
	for {
		logEntry, ok := <-reader.C
		if !ok {
			break
		}
		assert.Equal(t, logEntry.Line, []byte(fmt.Sprintf("oi-%d", i)))
		i++
	}
	assert.Equal(t, 10, i)
}

func TestStartConsumeWithOverFlow(t *testing.T) {
	logConsumer := newLogConsumer()

	r, w := io.Pipe()
	go logConsumer.StartConsume("mycontainer", r)

	enc := protoio.NewUint32DelimitedWriter(w, binary.BigEndian)

	for i := 0; i < MAX_LINES_BY_CONTAINER*2; i++ {
		err := enc.WriteMsg(&logdriver.LogEntry{
			Source:   "stdout",
			Line:     []byte(fmt.Sprintf("oi-%d", i)),
			TimeNano: time.Now().UnixNano(),
		})
		require.NoError(t, err)
	}

	time.Sleep(time.Second)
	reader := logConsumer.ReadLogs("mycontainer")

	i := 0
	for {
		logEntry, ok := <-reader.C
		if !ok {
			break
		}
		assert.Equal(t, logEntry.Line, []byte(fmt.Sprintf("oi-%d", i+100)))
		i++
	}
	assert.Equal(t, 100, i)

}
