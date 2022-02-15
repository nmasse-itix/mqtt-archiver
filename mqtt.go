package lib

import (
	"fmt"
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	MQTT_QOS_0 = 0
	MQTT_QOS_1 = 1
	MQTT_QOS_2 = 2
)

type MqttConfig struct {
	BrokerURL   string
	Username    string
	Password    string
	ClientID    string
	Timeout     time.Duration
	GracePeriod time.Duration
}

func SetMqttLogger(logger *log.Logger) {
	mqtt.CRITICAL = logger
	mqtt.ERROR = logger
	mqtt.WARN = logger
}

func NewMqttClient(config MqttConfig) (mqtt.Client, error) {
	if config.BrokerURL == "" {
		return nil, fmt.Errorf("MQTT broker URL is empty")
	}

	opts := mqtt.NewClientOptions()
	opts.AddBroker(config.BrokerURL)
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(config.Timeout)
	opts.SetOrderMatters(false)
	opts.SetCleanSession(false)
	opts.SetClientID(config.ClientID)
	opts.SetUsername(config.Username)
	opts.SetPassword(config.Password)

	client := mqtt.NewClient(opts)
	ct := client.Connect()
	if !ct.WaitTimeout(config.Timeout) {
		return nil, fmt.Errorf("mqtt: timeout waiting for connection")
	}

	return client, nil
}
