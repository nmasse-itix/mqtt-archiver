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
package cmd

import (
	"os"
	"os/signal"
	"syscall"

	mqttArchiver "github.com/nmasse-itix/mqtt-archiver"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// archiveCmd represents the archive command
var archiveCmd = &cobra.Command{
	Use:   "archive",
	Short: "Archive MQTT events from the broker to S3",
	Long:  `TODO`,
	Run: func(cmd *cobra.Command, args []string) {
		ok := true
		if viper.GetString("s3.endpoint") == "" {
			logger.Println("No S3 endpoint defined in configuration")
			ok = false
		}
		if viper.GetString("s3.accessKey") == "" {
			logger.Println("No S3 access key defined in configuration")
			ok = false
		}
		if viper.GetString("s3.secretKey") == "" {
			logger.Println("No S3 secret key defined in configuration")
			ok = false
		}
		if viper.GetString("s3.bucket") == "" {
			logger.Println("No S3 bucket name defined in configuration")
			ok = false
		}
		if viper.GetString("mqtt.broker") == "" {
			logger.Println("No MQTT broker defined in configuration")
			ok = false
		}
		if !ok {
			os.Exit(1)
		}

		archiver := mqttArchiver.Archiver{
			S3Config: mqttArchiver.S3Config{
				Endpoint:   viper.GetString("s3.endpoint"),
				AccessKey:  viper.GetString("s3.accessKey"),
				SecretKey:  viper.GetString("s3.secretKey"),
				UseSSL:     viper.GetBool("s3.ssl"),
				BucketName: viper.GetString("s3.bucket"),
			},
			MqttConfig: mqttArchiver.MqttConfig{
				BrokerURL:   viper.GetString("mqtt.broker"),
				Username:    viper.GetString("mqtt.username"),
				Password:    viper.GetString("mqtt.password"),
				ClientID:    viper.GetString("mqtt.clientId"),
				Timeout:     viper.GetDuration("mqtt.timeout"),
				GracePeriod: viper.GetDuration("mqtt.gracePeriod"),
			},
			SubscribePattern: viper.GetString("subscribePattern"),
			WorkingDir:       viper.GetString("workingDir"),
			FilterRegex:      viper.GetString("exclude"),
			Logger:           *logger,
		}

		// trap SIGINT and SIGTEM to gracefully stop
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

		logger.Println("Starting the archiving process...")
		err := archiver.StartArchive()
		if err != nil {
			logger.Fatalln(err)
		}
		logger.Println("Ready!")

		// Wait for SIGTERM or SIGINT
		sig := <-sigs
		logger.Printf("Received signal %s", sig)
		archiver.StopArchive()
	},
}

func init() {
	rootCmd.AddCommand(archiveCmd)

	// Each main feature gets its own default client id to prevent the replay
	// feature from colliding with the archive function
	viper.SetDefault("mqtt.clientId", "mqtt-archiver-archive")
}
