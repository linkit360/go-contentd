package config

import (
	"flag"
	"fmt"
	"os"
	"strconv"

	log "github.com/Sirupsen/logrus"
	"github.com/jinzhu/configor"

	"github.com/vostrok/contentd/service"
	"github.com/vostrok/db"
)

type ServerConfig struct {
	RPCPort  string `default:"50301" yaml:"rpc_port"`
	HttpPort string `default:"50302" yaml:"http_port"`
}
type AppConfig struct {
	Server   ServerConfig                 `yaml:"server"`
	Service  service.ContentServiceConfig `yaml:"service"`
	DbConf   db.DataBaseConfig            `yaml:"db"`
	Notifier service.NotifierConfig       `yaml:"notifier"`
}

func LoadConfig() AppConfig {
	cfg := flag.String("config", "dev/appconfig.yml", "configuration yml file")
	flag.Parse()
	var appConfig AppConfig

	if *cfg != "" {
		if err := configor.Load(&appConfig, *cfg); err != nil {
			log.WithField("config", err.Error()).Fatal("config load error")
			os.Exit(1)
		}
	}

	appConfig.Server.RPCPort = envString("PORT", appConfig.Server.RPCPort)
	appConfig.Server.HttpPort = envString("METRICS_PORT", appConfig.Server.HttpPort)

	appConfig.Notifier.Rbmq.Host = envString("RBMQ_HOST", appConfig.Notifier.Rbmq.Host)

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

func envInt(env string, fallback int) int {
	e := os.Getenv(env)
	d, err := strconv.Atoi(e)
	if err != nil {
		return fallback
	}
	return d
}

func envInt64(env string, fallback int64) int64 {
	e := os.Getenv(env)
	d, err := strconv.ParseInt(e, 10, 64)
	if err != nil {
		return fallback
	}
	return d
}
