package config

import (
	"flag"
	"os"
	"strconv"

	log "github.com/Sirupsen/logrus"
	"github.com/jinzhu/configor"

	"github.com/vostrok/contentd/service"
)

type ServerConfig struct {
	Port        string `default:"70401"`
	MetricsPort string `default:"70402" yaml:"metrics_port"`
}
type NewRelicConfig struct {
	AppName string `default:"contentd.linkit360.com"`
	License string `default:"4d635427ad90ca786ca2db6aa246ed651730b933"`
}
type AppConfig struct {
	Server   ServerConfig                 `yaml:"server"`
	NewRelic NewRelicConfig               `yaml:"newrelic"`
	Service  service.ContentServiceConfig `yaml:"service"`
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

	appConfig.Server.Port = envString("PORT", appConfig.Server.Port)
	appConfig.Server.MetricsPort = envString("METRICS_PORT", appConfig.Server.MetricsPort)

	appConfig.Service.TablePrefix = envString("TABLE_PREFIX", appConfig.Service.TablePrefix)
	appConfig.Service.UniqDays = envInt("UNIQ_DAYS", appConfig.Service.UniqDays)

	appConfig.Service.Notifier.Rbmq.Url = envString("MQ_URL", appConfig.Service.Notifier.Rbmq.Url)
	appConfig.Service.Notifier.Rbmq.ChanCap = envInt64("MQ_CHANNEL_GAP", appConfig.Service.Notifier.Rbmq.ChanCap)

	log.WithField("config", appConfig).Info("Config loaded")
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
