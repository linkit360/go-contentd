package config

import (
	"flag"
	"fmt"
	"os"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/jinzhu/configor"

	"github.com/linkit360/go-contentd/service"
	inmem "github.com/linkit360/go-inmem/rpcclient"
	"github.com/linkit360/go-utils/amqp"
)

type ServerConfig struct {
	RPCPort  string `default:"50301" yaml:"rpc_port"`
	HttpPort string `default:"50302" yaml:"http_port"`
}
type AppConfig struct {
	AppName     string                       `yaml:"app_name"`
	Server      ServerConfig                 `yaml:"server"`
	Service     service.ContentServiceConfig `yaml:"service"`
	InMemConfig inmem.ClientConfig           `yaml:"inmem_client"`
	Notifier    amqp.NotifierConfig          `yaml:"notifier"`
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
