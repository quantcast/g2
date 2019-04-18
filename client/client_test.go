package client

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	rt "github.com/quantcast/g2/pkg/runtime"
	"io"
	"net"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

type snapshot struct {
	filenames map[string]map[string]string
}

func loadSnapshot(directory string) (s *snapshot, err error) {
	manifest, err := os.OpenFile(directory+"/manifest", os.O_RDONLY, 0)

	if err != nil {
		return
	}

	scanner := bufio.NewScanner(manifest)

	filenames := make(map[string]map[string]string)
	addresses := make(map[string]string)

	for scanner.Scan() {
		line := scanner.Bytes()

		parts := strings.Split(string(line), " ")

		if len(parts) != 2 {
			panic(fmt.Sprintf("Expected space delimited input, saw: \"%s\"", line))
		}

		source, address := parts[0], parts[1]

		addresses[source] = address
	}

	convos := [][]string{
		[]string{"client", "server"},
		[]string{"server", "client"},
		[]string{"worker", "server"},
		[]string{"server", "worker"},
	}

	for _, components := range convos {
		sender, recipient := components[0], components[1]

		if _, ok := filenames[sender]; !ok {
			filenames[sender] = make(map[string]string)
		}

		filenames[sender][recipient] = fmt.Sprintf("%s/%s-%s", directory, addresses[sender], addresses[recipient])
	}

	s = &snapshot{filenames}

	return
}

func (s *snapshot) load(sender, recipient string) (r io.Reader, err error) {
	if fn, ok := s.filenames[sender][recipient]; ok {
		var convo *os.File

		convo, err = os.OpenFile(fn, os.O_RDONLY, 0)

		if err != nil {
			return
		}

		defer convo.Close()

		buffer := &bytes.Buffer{}

		_, err = buffer.ReadFrom(convo)

		r = buffer
	} else {
		err = errors.New(fmt.Sprintf("%s -> %s missing from snapshot", sender,
			recipient))
	}

	return
}

// Replay the specified part of the conversation to the given writer
//
// This happens blindly in the background but panics if any error occurs.
func (s *snapshot) replay(stream io.Writer, sender, recipient string) error {
	convo, err := s.load(sender, recipient)

	if err == nil {
		go func() {
			_, err := io.Copy(stream, convo)

			if err != nil {
				panic(err)
			}
		}()
	} else {
		panic(err)
	}

	return err
}

func verifyObservation(errors chan error, observation io.Reader, expectation io.Reader) {
	running := true

	observed := make([]byte, 1024)
	expected := make([]byte, 1024)

	var o, e int
	var err error

	for running {
		o, err = observation.Read(observed)

		if err != nil {
			errors <- fmt.Errorf("unexpected error reading client input: %s", err)

			break
		}

		e, err = io.ReadFull(expectation, expected[0:o])

		if err == io.EOF {
			running = false
		}

		if err != nil {
			errors <- fmt.Errorf("unexpected error reading snapshot input: %s", err)

			break
		}

		fmt.Printf("read from snapshot\n")

		if !reflect.DeepEqual(observed[0:o], expected[0:e]) {
			errors <- fmt.Errorf("input expectation mismatch: \n%s\n\n%s\n",
				hex.Dump(observed[0:o]), hex.Dump(expected[0:e]))

			break
		}

		fmt.Printf("compared client and snapshot input\n")
	}
}

func (s *snapshot) validate(errors chan error, observation io.Reader, sender, recipient string) {
	expectation, err := s.load(sender, recipient)

	if err != nil {
		errors <- err

		return
	}

	go verifyObservation(errors, expectation, observation)
}

// Read from the given reader in the background until there was an error
func drain(observed io.Reader) {
	go func() {
		var err error

		buf := make([]byte, 1024)

		for err == nil {
			_, err = observed.Read(buf)
		}

		if err != io.EOF {
			panic(err)
		}
	}()
}

func TestClose(test *testing.T) {

	client, _ := net.Pipe()

	gearmanc := NewConnected(client)

	if gearmanc.Close() != nil {
		test.Fatalf("expected no error in closing connected client")
	}

	if gearmanc.Close() == nil {
		test.Fatalf("expected error closing disconnected client")
	}
}

func TestSnapshot(test *testing.T) {
	snapshotTest(test, 0, 5)
}

func TestSnapshotDelayed(test *testing.T) {
	snapshotTest(test, 5, 5)
}

func snapshotTest(test *testing.T, delaySubmitMs uint8, delayProcessMs uint8) {
	client, server := net.Pipe()

	snapshot, err := loadSnapshot("perl")

	if err != nil {
		test.Fatalf("error loading snapshot: %s\n", err)
	}

	// This has to be done in another go-routine since all of the reads/writes
	// are synchronous
	gearmanClient := NewConnected(client)
	gearmanClient.delaySubmitMs = delaySubmitMs
	gearmanClient.ErrorHandler = func(err error) {
		test.Fatalf("Client error: %s", err.Error())
	}

	if err = snapshot.replay(server, "server", "client"); err != nil {
		test.Fatalf("error loading snapshot: %s", err)
	}

	drain(server)

	payload := []byte("1548268329.11348:x")

	errors := make(chan error)

	ch := make(chan bool, 1)
	defer close(ch)

	timer := time.NewTimer(1 * time.Second)
	defer timer.Stop()

	go func() {
		if _, err := gearmanClient.Do("test", payload, rt.JobNormal, func(r *Response) {
			if !reflect.DeepEqual(payload, r.Data) {
				errors <- fmt.Errorf("\nexpected:\n%s\nobserved:\n%s\n",
					hex.Dump(payload), hex.Dump(r.Data))
			}
			close(errors)
			ch <- true
		}); err != nil {
			errors <- fmt.Errorf("\nError:%v", err.Error())
		}
	}()

	go func() {
		for err := range errors {
			test.Fatalf("error: %s", err)
		}
	}()

	select {
	case <-ch:
		fmt.Println("Test Finished")
	case <-timer.C:
		test.Fatalf("Timeout")
	}

}
