/*
Copyright © 2022 Nicolas MASSE

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/
package lib

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// An S3Config represents the mandatory parameters to connect to an S3 service
type S3Config struct {
	Endpoint   string // S3 endpoint. Format is "hostname:port"
	AccessKey  string // S3 access key (or username)
	SecretKey  string // S3 access key (or token)
	UseSSL     bool   // Enable/disable the use of TLS to connect to S3
	BucketName string // The name of the bucket where to store archives
}

// An EventLogEntry represents a single event received from the MQTT broker
type EventLogEntry struct {
	Version   int       `json:"v"`            // Version number (in case the format changes in the future)
	Timestamp time.Time `json:"ts,omitempty"` // An optional timestamp
	Topic     string    `json:"topic"`        // The topic this event comes from
	Payload   []byte    `json:"data"`         // The actual data (is base64 encoded in the JSON structure)
}

// An Archiver represents the process of archiving MQTT events to S3.
// The Archiver will subscribe to topics designated by the SubscribePattern,
// save events to a JSON file in the WorkingDir.
// It will rotate the JSON file every day, compress it and send it to S3.
// The files are stored in S3 in a folder per year.
type Archiver struct {
	S3Config         S3Config           // credentials to connect to S3
	MqttConfig       MqttConfig         // credentials to connect to MQTT
	WorkingDir       string             // location to store JSON files
	Logger           log.Logger         // a logger
	SubscribePattern string             // the pattern (ie. "#") to subscribe to
	currentFilename  string             // the current JSON filename
	wg               sync.WaitGroup     // a wait group to keep track of each running go routine
	client           mqtt.Client        // the MQTT client
	done             chan bool          // channel to trigger the end of the archiving process
	eventLog         chan EventLogEntry // channel to send events from the MQTT go routines to the go routine that write the JSON file
}

const (
	// JSON filename format (one file per day)
	logfileFormat = "20060102.json"
)

// StartArchive starts the archiving process. It is not safe to call
// this method multiple times on the same object.
func (archiver *Archiver) StartArchive() error {
	archiver.done = make(chan bool)
	archiver.eventLog = make(chan EventLogEntry, 10)

	// connect to MQTT server
	SetMqttLogger(&archiver.Logger)
	archiver.Logger.Println("Connecting to MQTT server...")
	var err error
	archiver.client, err = NewMqttClient(archiver.MqttConfig)
	if err != nil {
		return err
	}

	// subscribe to topics
	archiver.Logger.Printf("Subscribing to topics %s...", archiver.SubscribePattern)
	st := archiver.client.Subscribe(archiver.SubscribePattern, MQTT_QOS_2, archiver.processMessage)
	if !st.WaitTimeout(archiver.MqttConfig.Timeout) {
		return fmt.Errorf("mqtt: timeout waiting for subscribe")
	}

	// The events are written to the JSON files by a dedicated go routine
	// to avoid synchronization issues or race conditions.
	go archiver.eventLogWriter()

	return nil
}

// StopArchive stops the archiving process. It is not safe to call
// this method multiple times on the same object.
func (archiver *Archiver) StopArchive() {
	// First, disconnect from the MQTT broker and leave a grace time period
	// for the MQTT library to process the last inflight messages.
	archiver.Logger.Println("Closing connection to the broker...")
	archiver.client.Disconnect(uint(archiver.MqttConfig.GracePeriod.Milliseconds()))

	// Signal the eventLogWriter method that it needs to stop
	archiver.done <- true

	// Then, wait for all go routines to complete.
	archiver.Logger.Println("Waiting for all goroutines to complete...")
	archiver.wg.Wait()
}

// eventLogWriter receives events from the eventLog channel and stores them
// to a JSON file.
func (archiver *Archiver) eventLogWriter() {
	archiver.wg.Add(1)
	defer archiver.wg.Done()

	// Open the JSON file
	var fd *os.File
	var err error
	if archiver.currentFilename == "" {
		archiver.currentFilename = time.Now().UTC().Format(logfileFormat)
		fd, err = archiver.openLogFile(archiver.currentFilename)
		if err != nil {
			archiver.Logger.Println(err)
			os.Exit(1)
		}
		defer fd.Close()
	}

	// Before actually writing the JSON file, catch up with any file that might
	// have been left by a previous run.
	// The catchUp method will discover files that need to be compressed and
	// sent to S3, carefully excluding the current file (archiver.currentFilename)
	// Once the file list has been built, a go routine will take care of the
	// actual operations asynchronously.
	// It is important to build the file list after having computed the currentFilename
	// but before writing events since we do not want to have a race condition
	// between the catchUp job and the log rotation...
	archiver.catchUp()

	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-archiver.done: // We need to stop
			return
		case entry := <-archiver.eventLog:
			if fd == nil {
				// It can happen if for instance the log rotation fails
				archiver.Logger.Println("A message has been lost because the file descriptor is nil")
				continue
			}

			jsonEntry, err := json.Marshal(entry)
			if err != nil {
				archiver.Logger.Println(err)
				continue
			}

			_, err = fd.WriteString(string(jsonEntry))
			if err != nil {
				archiver.Logger.Println(err)
				continue
			}

			_, err = fd.WriteString(string("\n"))
			if err != nil {
				archiver.Logger.Println(err)
				continue
			}

			err = fd.Sync()
			if err != nil {
				archiver.Logger.Println(err)
				continue
			}
		case t := <-tick.C:
			newFileName := t.UTC().Format(logfileFormat)
			if newFileName != archiver.currentFilename {
				archiver.Logger.Printf("Rotating log file %s -> %s ...", archiver.currentFilename, newFileName)
				err = fd.Close()
				if err != nil {
					archiver.Logger.Println(err)
				}
				fd, err = archiver.openLogFile(newFileName)
				if err != nil {
					archiver.Logger.Println(err)
					continue
				}
				go archiver.compressAndSendToS3(archiver.currentFilename)
				archiver.currentFilename = newFileName
			}
		}
	}
}

// catchUp discovers files that need to be compressed and sent to S3,
// carefully excluding the current file (archiver.currentFilename).
// Once the file list has been built, a go routine takes care of the
// actual operations asynchronously.
func (archiver *Archiver) catchUp() {
	var toCompress []string
	var toSend []string

	var dedup map[string]bool = make(map[string]bool)

	matches, err := filepath.Glob(path.Join(archiver.WorkingDir, "*.json"))
	if err != nil {
		archiver.Logger.Println(err)
		return
	}
	for _, match := range matches {
		_, file := filepath.Split(match)
		if file == archiver.currentFilename {
			continue
		}
		dedup[file] = true
		toCompress = append(toCompress, file)
	}

	matches, err = filepath.Glob(path.Join(archiver.WorkingDir, "*.json.gz"))
	if err != nil {
		archiver.Logger.Println(err)
		return
	}
	for _, match := range matches {
		_, file := filepath.Split(match)
		_, seen := dedup[file+".gz"]

		// Never trust a .gz file if there is also the corresponding .json file
		// the compression process might have failed and left a corrupted .gz file
		if !seen {
			toSend = append(toSend, file)
		}
	}

	go archiver.doCatchUp(toCompress, toSend)
}

// doCatchUp sends already compressed files to S3 and compress + send the
// remaining files
func (archiver *Archiver) doCatchUp(toCompress, toSend []string) {
	if len(toCompress)+len(toSend) > 0 {
		archiver.Logger.Printf("Catching up with %d files...", len(toCompress)+len(toSend))
	}

	for _, file := range toCompress {
		archiver.compressAndSendToS3(file)
	}
	for _, file := range toSend {
		archiver.sendToS3(file)
	}
}

// compressFile compresses a file using gzip
func (archiver *Archiver) compressFile(source, target string) error {
	tfd, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
	if err != nil {
		return err
	}
	defer tfd.Close()

	sfd, err := os.OpenFile(source, os.O_RDONLY, 0755)
	if err != nil {
		return err
	}
	defer sfd.Close()

	gzwr := gzip.NewWriter(tfd)
	defer gzwr.Close()

	_, err = io.Copy(gzwr, sfd)
	if err != nil {
		return err
	}

	err = os.Remove(source)
	if err != nil {
		return err
	}

	return nil
}

// compressAndSendToS3 compresses a JSON file and sends it to S3
func (archiver *Archiver) compressAndSendToS3(fileName string) error {
	logFilePath := path.Join(archiver.WorkingDir, fileName)
	err := archiver.compressFile(logFilePath, logFilePath+".gz")
	if err != nil {
		archiver.Logger.Println(err)
		return err
	}

	// the sendToS3 method can take very long time to terminate (days)
	// because of the embedded backoff mechanism
	go archiver.sendToS3(fileName + ".gz")

	return nil
}

// sendToS3 takes a compressed JSON file and sends it to S3. An exponential
// backoff retry mechanism takes care of possible errors during the process.
// Note: in case of errors, the sendToS3 method can take very long time to
// terminate (days).
func (archiver *Archiver) sendToS3(fileName string) {
	logFilePath := path.Join(archiver.WorkingDir, fileName)

	var delay time.Duration
	for i := 0; i < 9; i++ {
		ctx := context.Background()

		if delay != 0 {
			archiver.Logger.Printf("Backing off %s before sending %s...", delay.String(), fileName)
		}

		time.Sleep(delay)

		// Exponential backoff
		// 5s, 30s, 3m, 18m, ~2h, 10h, ~3d, ~2w
		if delay == 0 {
			delay = 5 * time.Second
		} else {
			delay = delay * 6
		}

		client, err := minio.New(archiver.S3Config.Endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(archiver.S3Config.AccessKey, archiver.S3Config.SecretKey, ""),
			Secure: archiver.S3Config.UseSSL,
		})
		if err != nil {
			archiver.Logger.Println(err)
			continue
		}

		exists, err := client.BucketExists(ctx, archiver.S3Config.BucketName)
		if err != nil {
			archiver.Logger.Println(err)
			continue
		}
		if !exists {
			err = client.MakeBucket(ctx, archiver.S3Config.BucketName, minio.MakeBucketOptions{})
			if err != nil {
				archiver.Logger.Println(err)
				continue
			}
		}

		targetPath := path.Join(string(fileName[0:4]), fileName)
		_, err = client.FPutObject(ctx, archiver.S3Config.BucketName, targetPath, logFilePath, minio.PutObjectOptions{ContentType: "applicaton/gzip"})
		if err != nil {
			archiver.Logger.Println(err)
			continue
		}

		err = os.Remove(logFilePath)
		if err != nil {
			archiver.Logger.Println(err)
			// Not a critical error
		}

		break
	}
}

// openLogFile opens the JSON file for writing.
func (archiver *Archiver) openLogFile(fileName string) (*os.File, error) {
	archiver.Logger.Printf("Opening log file %s...", fileName)
	logFilePath := path.Join(archiver.WorkingDir, fileName)
	return os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0755)
}

// processMessage is the callback routine called by the MQTT library to process
// events.
func (archiver *Archiver) processMessage(c mqtt.Client, m mqtt.Message) {
	if m.Retained() {
		return
	}

	archiver.wg.Add(1)
	defer archiver.wg.Done()
	entry := EventLogEntry{
		Version:   1,
		Timestamp: time.Now().UTC(),
		Topic:     m.Topic(),
		Payload:   m.Payload(),
	}
	archiver.eventLog <- entry
}
