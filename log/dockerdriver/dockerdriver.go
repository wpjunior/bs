package dockerdriver

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"syscall"

	"github.com/alecthomas/repr"
	"github.com/docker/docker/daemon/logger"
	"github.com/docker/go-plugins-helpers/sdk"
	protoio "github.com/gogo/protobuf/io"
	"github.com/pkg/errors"
	"github.com/tonistiigi/fifo"
)

type DockerDriver interface {
	Start() error
}

type dockerDriver struct {
	handler  *sdk.Handler
	consumer LogConsumer
}

func (d *dockerDriver) Start() error {
	l, err := net.Listen("tcp", "127.0.0.1:3002")
	if err != nil {
		return err
	}
	return d.handler.Serve(l)
}

func (d *dockerDriver) startLogging(r *StartLoggingRequest) error {
	f, err := fifo.OpenFifo(context.Background(), r.File, syscall.O_RDONLY, 0700)
	if err != nil {
		return errors.Wrapf(err, "error opening logger fifo: %q", r.File)
	}

	// it's important to use one goroutine per container
	go d.consumer.StartConsume(r.Info.ContainerID, f)

	return nil
}

func (d *dockerDriver) stopLogging(r *StopLoggingRequest) error {
	fmt.Println("stop logging")
	repr.Println(r)
	return nil
}

func (d *dockerDriver) readLogs(req *ReadLogsRequest, w http.ResponseWriter) error {
	enc := protoio.NewUint32DelimitedWriter(w, binary.BigEndian)

	flusher := w.(http.Flusher)
	reader := d.consumer.ReadLogs(req.Info.ContainerID)

	defer enc.Close()

	for {
		logEntry, ok := <-reader.C
		if !ok {
			break
		}

		logEntry.Line = append(logEntry.Line, '\n')
		err := enc.WriteMsg(&logEntry)
		if err != nil {
			// TODO: send message to reader to stop reading
			fmt.Printf("Failed to push logs on stream, containerID: %s, error: %s", req.Info.ContainerID, err.Error())
			return nil
		}
		flusher.Flush()
	}

	return nil
}

func New() DockerDriver {
	handler := sdk.NewHandler(`{"Implements": ["LogDriver"]}`)
	driver := &dockerDriver{
		handler:  &handler,
		consumer: newLogConsumer(),
	}

	handler.HandleFunc("/LogDriver.StartLogging", func(w http.ResponseWriter, r *http.Request) {
		var req StartLoggingRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		err := driver.startLogging(&req)
		respond(err, w)
	})

	handler.HandleFunc("/LogDriver.StopLogging", func(w http.ResponseWriter, r *http.Request) {
		var req StopLoggingRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		err := driver.stopLogging(&req)
		respond(err, w)
	})

	handler.HandleFunc("/LogDriver.Capabilities", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("get log capabilities")

		json.NewEncoder(w).Encode(&CapabilitiesResponse{
			Cap: logger.Capability{ReadLogs: true},
		})
	})

	handler.HandleFunc("/LogDriver.ReadLogs", func(w http.ResponseWriter, r *http.Request) {
		var req ReadLogsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/x-json-stream")
		err := driver.readLogs(&req, w)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

	})
	return driver
}

type response struct {
	Err string
}

func respond(err error, w http.ResponseWriter) {
	var res response
	if err != nil {
		res.Err = err.Error()
	}
	json.NewEncoder(w).Encode(&res)
}
