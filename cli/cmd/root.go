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
	"log"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type timeArg struct {
	time time.Time
}

func (t *timeArg) String() string {
	return t.time.String()
}

func (t *timeArg) Set(val string) error {
	var err error
	if val == "now" {
		t.time = time.Now().UTC()
	} else {
		t.time, err = time.ParseInLocation("2006-01-02T15:04:05", val, time.UTC)
	}
	return err
}

func (t *timeArg) Type() string {
	return "time"
}

var follow bool = false
var from, to timeArg
var prefix string
var cfgFile string
var logger *log.Logger

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "mqtt-archiver",
	Short: "Archives and replays events from MQTT servers",
	Long:  `TODO`,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	// Initializes a new logger without timestamps
	logger = log.New(os.Stderr, "", 0)

	// Set default configuration
	viper.SetDefault("mqtt.timeout", 30*time.Second)
	viper.SetDefault("mqtt.gracePeriod", 5*time.Second)
	viper.SetDefault("subscribePattern", "#")
	viper.SetDefault("s3.ssl", true)
	viper.SetDefault("workingDir", ".")

	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $PWD/mqtt-archiver.yaml)")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Search working directory with name "mqtt-archiver" (without extension).
		viper.AddConfigPath(".")
		viper.SetConfigName("mqtt-archiver")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}
