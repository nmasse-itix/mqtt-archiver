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

type S3Config struct {
	Endpoint   string
	AccessKey  string
	SecretKey  string
	UseSSL     bool
	BucketName string
}

type EventLogEntry struct {
	Version   int
	Timestamp time.Time
	Topic     string
	Payload   []byte
}

type Archiver struct {
	S3Config         S3Config
	MqttConfig       MqttConfig
	WorkingDir       string
	Logger           log.Logger
	SubscribePattern string
	currentFilename  string
	wg               sync.WaitGroup
	client           mqtt.Client
	done             chan bool
	eventLog         chan EventLogEntry
}

const (
	logfileFormat = "20060102.json"
)

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

	go archiver.eventLogWriter()

	return nil
}

func (archiver *Archiver) StopArchive() {
	archiver.Logger.Println("Closing connection to the broker...")
	archiver.client.Disconnect(uint(archiver.MqttConfig.GracePeriod.Milliseconds()))
	archiver.Logger.Println("Waiting for all goroutines to complete...")
	archiver.done <- true
	archiver.wg.Wait()
}

func (archiver *Archiver) eventLogWriter() {
	archiver.wg.Add(1)
	defer archiver.wg.Done()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()

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

	archiver.catchUp()

	for {
		select {
		case <-archiver.done:
			return
		case entry := <-archiver.eventLog:
			if fd == nil {
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

func (archiver *Archiver) catchUp() {
	var toCompress []string
	var toSend []string

	matches, err := filepath.Glob(path.Join(archiver.WorkingDir, "*.json.gz"))
	if err != nil {
		archiver.Logger.Println(err)
		return
	}
	for _, match := range matches {
		_, file := filepath.Split(match)
		toSend = append(toSend, file)
	}

	matches, err = filepath.Glob(path.Join(archiver.WorkingDir, "*.json"))
	if err != nil {
		archiver.Logger.Println(err)
		return
	}
	for _, match := range matches {
		_, file := filepath.Split(match)
		if file == archiver.currentFilename {
			continue
		}
		toCompress = append(toCompress, file)
	}

	go archiver.doCatchUp(toCompress, toSend)
}

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

func (archiver *Archiver) compressAndSendToS3(fileName string) {
	logFilePath := path.Join(archiver.WorkingDir, fileName)
	err := archiver.compressFile(logFilePath, logFilePath+".gz")
	if err != nil {
		archiver.Logger.Println(err)
		return
	}
	archiver.sendToS3(fileName + ".gz")
}

func (archiver *Archiver) sendToS3(fileName string) {
	logFilePath := path.Join(archiver.WorkingDir, fileName)

	var delay time.Duration
	for i := 0; i < 8; i++ {
		ctx := context.Background()

		if delay != 0 {
			archiver.Logger.Printf("Backing off %s before sending %s...", delay.String(), fileName)
		}

		time.Sleep(delay)

		// Exponential backoff
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

func (archiver *Archiver) openLogFile(fileName string) (*os.File, error) {
	archiver.Logger.Printf("Opening log file %s...", archiver.currentFilename)
	logFilePath := path.Join(archiver.WorkingDir, fileName)
	return os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0755)
}

func (archiver *Archiver) processMessage(c mqtt.Client, m mqtt.Message) {
	if m.Retained() {
		return
	}

	archiver.wg.Add(1)
	defer archiver.wg.Done()
	//fmt.Printf("%d: %s: %s\n", m.MessageID(), m.Topic(), (string)(m.Payload()))
	entry := EventLogEntry{
		Version:   1,
		Timestamp: time.Now().UTC(),
		Topic:     m.Topic(),
		Payload:   m.Payload(),
	}
	archiver.eventLog <- entry
}
