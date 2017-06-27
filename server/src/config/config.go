package config

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/jinzhu/configor"
	log "github.com/sirupsen/logrus"

	"github.com/linkit360/go-contentd/server/src/service"
	mid "github.com/linkit360/go-mid/rpcclient"
	"github.com/linkit360/go-utils/amqp"
)

type ServerConfig struct {
	Host     string `default:"127.0.0.1" yaml:"host"`
	RPCPort  string `default:"50301" yaml:"rpc_port"`
	HttpPort string `default:"50302" yaml:"http_port"`
}
type AppConfig struct {
	AppName   string                       `yaml:"app_name"`
	Server    ServerConfig                 `yaml:"server"`
	MidConfig mid.ClientConfig             `yaml:"mid_client"`
	Service   service.ContentServiceConfig `yaml:"service"`
	Notifier  amqp.NotifierConfig          `yaml:"notifier"`
}

func LoadConfig() AppConfig {
	cfg := flag.String("config", "dev/contentd.yml", "configuration yml file")
	flag.Parse()
	var appConfig AppConfig

	if *cfg != "" {
		if err := configor.Load(&appConfig, *cfg); err != nil {
			log.WithField("config", err.Error()).Fatal("config load error")
			os.Exit(1)
		}
	}
	if appConfig.AppName == "" {
		log.Fatal("app name must be defiled as <host>-<name>")
	}
	if strings.Contains(appConfig.AppName, "-") {
		log.Fatal("app name must be without '-' : it's not a valid metric name")
	}
	appConfig.Server.RPCPort = envString("PORT", appConfig.Server.RPCPort)
	appConfig.Server.HttpPort = envString("HTTP_PORT", appConfig.Server.HttpPort)

	log.WithField("config", fmt.Sprintf("%#v", appConfig)).Info("Config loaded")
	return appConfig
}

func envString(env, fallback string) string {
	e := os.Getenv(env)
	if e == "" {
		return fallback
	}
	return e
}
