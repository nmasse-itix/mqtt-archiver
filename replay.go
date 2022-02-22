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
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const (
	// Maximum length of a JSON entry (bytes)
	MaxTokenSize int = 1024 * 1024
)

// A ReplayerConfig holds the configuration data of a Replayer
type ReplayerConfig struct {
	S3Config    S3Config    // credentials to connect to S3
	MqttConfig  MqttConfig  // credentials to connect to MQTT
	WorkingDir  string      // location to store JSON files
	Follow      bool        // when listing archives, wait for new archives as they are produced
	Logger      *log.Logger // a logger
	From        time.Time   // begining of the replay period
	To          time.Time   // end of the replay period
	TopicPrefix string      // prefix topic with this string
}

// A Replayer replays events from JSON archives to the MQTT broker
type Replayer struct {
	Config     ReplayerConfig // the replayer public configuration
	wg         sync.WaitGroup // a wait group to keep track of each running go routine
	s3Client   *minio.Client  // the s3 client
	mqttClient mqtt.Client    // the MQTT client
	done       chan bool      // a channel to signal the replayer it must ends gracefully
}

// An Archive represents an archive that has been opened (file descriptor is open).
// Any method receiving this structure is responsible for calling Reader.Close().
type Archive struct {
	Timestamp time.Time
	Reader    io.ReadCloser
	FileName  string
}

// NewReplayer builds a replayer by its public configuration
func NewReplayer(c ReplayerConfig) (*Replayer, error) {
	var replayer Replayer = Replayer{
		Config: c,
	}

	var err error
	replayer.s3Client, err = minio.New(replayer.Config.S3Config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(replayer.Config.S3Config.AccessKey, replayer.Config.S3Config.SecretKey, ""),
		Secure: replayer.Config.S3Config.UseSSL,
	})
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	exists, err := replayer.s3Client.BucketExists(ctx, replayer.Config.S3Config.BucketName)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("s3 bucket does not exist")
	}

	// There are two consumers of this channel (ListArchives and StartReplay)
	replayer.done = make(chan bool, 2)

	return &replayer, nil
}

// ListArchives sends a list of available archives (and potential errors)
// through the files (and errors) channels.
// If replayer.Config.Follow is false, the eol channel is used to signal
// the end of the list.
func (replayer *Replayer) ListArchives(files chan Archive, errors chan error, eol chan struct{}) {
	replayer.wg.Add(1)
	defer replayer.wg.Done()

	i := replayer.Config.From

	var end time.Time = replayer.Config.To
	var loose bool = false
main:
	for {
		var retries = 5
		for i.Before(end) {
			fd, err := replayer.OpenArchive(i)
			if err != nil {
				errors <- err
			} else if fd != nil {
				var archive Archive = Archive{
					Timestamp: i,
					FileName:  i.Format(logfileFormat),
					Reader:    fd,
				}
				files <- archive
			} else if loose && retries > 0 {
				// When following archive production, the productor might not
				// had the time to rotate its archives. Retry for a few seconds...
				retries--
				time.Sleep(time.Second)
				continue
			}

			i = i.Add(rotationInterval)
		}

		if !replayer.Config.Follow {
			eol <- struct{}{} // Signal end of list
			break
		}

		// End or continue after a one second pause
		select {
		case <-replayer.done:
			break main
		case <-time.After(time.Second):

		}

		end = time.Now().UTC()
		loose = true
	}
}

// StopReplay initiates a graceful stop of the replay process.
func (replayer *Replayer) StopReplay() {
	replayer.done <- true
	replayer.done <- true
	replayer.wg.Wait()
}

// StartReplay starts the replay process.
func (replayer *Replayer) StartReplay() {
	var err error

	replayer.wg.Add(1)
	defer replayer.wg.Done()

	// initialize the MQTT library
	SetMqttLogger(replayer.Config.Logger)
	replayer.mqttClient, err = NewMqttClient(replayer.Config.MqttConfig, true)
	if err != nil {
		replayer.Config.Logger.Println(err)
		return
	}

	files := make(chan Archive)
	errors := make(chan error)
	eol := make(chan struct{})
	go replayer.ListArchives(files, errors, eol)

main:
	for {
		var archive Archive
		select {
		case <-eol:
			break main
		case err := <-errors:
			replayer.Config.Logger.Println(err)
			continue
		case archive = <-files:
			err = replayer.ReplayArchive(archive)
			if err != nil {
				replayer.Config.Logger.Println(err)
			}
		case <-replayer.done:
			break main
		}

	}
}

// ReplayArchive replays a specific archive.
func (replayer *Replayer) ReplayArchive(archive Archive) error {
	defer archive.Reader.Close()

	replayer.Config.Logger.Printf("Replaying archive %s...", archive.FileName)

	var buffer []byte = make([]byte, MaxTokenSize)
	scanner := bufio.NewScanner(archive.Reader)
	scanner.Buffer(buffer, MaxTokenSize)
	for scanner.Scan() {
		var evt EventLogEntry
		err := json.Unmarshal(scanner.Bytes(), &evt)
		if err != nil {
			return err
		}

		var topic string = evt.Topic
		if replayer.Config.TopicPrefix != "" {
			topic = replayer.Config.TopicPrefix + topic
		}
		token := replayer.mqttClient.Publish(topic, MQTT_QOS_2, false, evt.Payload)
		if !token.WaitTimeout(replayer.Config.MqttConfig.Timeout) {
			return fmt.Errorf("timeout")
		}
	}

	return nil
}

// OpenArchive opens the archive containing to a specific point in time.
// It handle the following cases:
// - .json file in the working directory
// - .json.gz file in the working directory
// - .json.gz file in the S3 bucket
func (replayer *Replayer) OpenArchive(moment time.Time) (io.ReadCloser, error) {
	fileName := moment.Format(logfileFormat)
	logFilePath := path.Join(replayer.Config.WorkingDir, fileName)
	fd, err := os.OpenFile(logFilePath, os.O_RDONLY, 0755)
	if err == nil {
		return fd, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	fileName += ".gz"
	fd, err = os.OpenFile(logFilePath, os.O_RDONLY, 0755)
	if err == nil {
		return gzip.NewReader(fd)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	ctx := context.Background()
	s3FileName := path.Join(string(fileName[0:4]), fileName)
	obj, err := replayer.s3Client.GetObject(ctx, replayer.Config.S3Config.BucketName, s3FileName, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	_, err = obj.Stat()
	if err == nil {
		return gzip.NewReader(obj)
	}

	var s3err minio.ErrorResponse
	if errors.As(err, &s3err) && s3err.Code == "NoSuchKey" {
		return nil, nil
	}

	return nil, err
}
