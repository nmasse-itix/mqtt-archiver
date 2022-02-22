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
	"fmt"
	"os"

	mqttArchiver "github.com/nmasse-itix/mqtt-archiver"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// listCmd represents the list command
var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List available archives",
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
		if from.time.IsZero() {
			logger.Println("Please specify the beginning of the replay period")
			ok = false
		}
		if to.time.IsZero() {
			to.Set("now")
		}
		if !ok {
			os.Exit(1)
		}

		config := mqttArchiver.ReplayerConfig{
			S3Config: mqttArchiver.S3Config{
				Endpoint:   viper.GetString("s3.endpoint"),
				AccessKey:  viper.GetString("s3.accessKey"),
				SecretKey:  viper.GetString("s3.secretKey"),
				UseSSL:     viper.GetBool("s3.ssl"),
				BucketName: viper.GetString("s3.bucket"),
			},
			WorkingDir: viper.GetString("workingDir"),
			Logger:     logger,
			Follow:     follow,
			From:       from.time,
			To:         to.time,
		}
		replayer, err := mqttArchiver.NewReplayer(config)
		if err != nil {
			logger.Fatalln(err)
		}

		files := make(chan mqttArchiver.Archive)
		errors := make(chan error)
		eol := make(chan struct{})

		go replayer.ListArchives(files, errors, eol)

		for {
			select {
			case <-eol:
				return
			case err := <-errors:
				logger.Println(err)
			case file := <-files:
				file.Reader.Close()
				fmt.Println(file.FileName)
			}
		}

	},
}

func init() {
	listCmd.Flags().BoolVarP(&follow, "follow", "f", false, "list archives as they are produced")
	listCmd.Flags().Var(&from, "from", "beginning of list period")
	listCmd.Flags().Var(&to, "to", "end of list period")

	rootCmd.AddCommand(listCmd)

}
