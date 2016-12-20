package config

import (
	"flag"
	"fmt"
	"os"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/jinzhu/configor"

	"github.com/vostrok/contentd/service"
	inmem "github.com/vostrok/inmem/rpcclient"
)

type ServerConfig struct {
	RPCPort  string `default:"50301" yaml:"rpc_port"`
	HttpPort string `default:"50302" yaml:"http_port"`
}
type AppConfig struct {
	MetricInstancePrefix string                       `yaml:"metric_instance_prefix"`
	AppName              string                       `yaml:"app_name"`
	Server               ServerConfig                 `yaml:"server"`
	Service              service.ContentServiceConfig `yaml:"service"`
	InMemConfig          inmem.RPCClientConfig        `yaml:"inmem_client"`
	Notifier             service.NotifierConfig       `yaml:"notifier"`
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
	if appConfig.MetricInstancePrefix == "" {
		log.Fatal("metric_instance_prefix be defiled as <host>_<name>")
	}
	if strings.Contains(appConfig.MetricInstancePrefix, "-") {
		log.Fatal("metric_instance_prefix be without '-' : it's not a valid metric name")
	}

	appConfig.Server.RPCPort = envString("PORT", appConfig.Server.RPCPort)
	appConfig.Server.HttpPort = envString("METRICS_PORT", appConfig.Server.HttpPort)

	appConfig.Notifier.Rbmq.Conn.Host = envString("RBMQ_HOST", appConfig.Notifier.Rbmq.Conn.Host)

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
