package service

import (
	"database/sql"
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
)

type DataBaseConfig struct {
	ConnMaxLifetime  int    `default:"-1" yaml:"conn_ttl"`
	MaxOpenConns     int    `default:"1" yaml:"max_conns"`
	ReconnectTimeout int    `default:"10" yaml:"timeout"`
	User             string `default:""`
	Pass             string `default:""`
	Port             string `default:""`
	Name             string `default:""`
	Host             string `default:""`
	SSLMode          string `default:"disabled"`
	TablePrefix      string `default:""`
}

func (dbConfig DataBaseConfig) GetConnStr() string {
	dsn := "postgres://" +
		dbConfig.User + ":" +
		dbConfig.Pass + "@" +
		dbConfig.Host + ":" +
		dbConfig.Port + "/" +
		dbConfig.Name + "?sslmode=" +
		dbConfig.SSLMode
	return dsn
}

func initDatabase(dbConf DataBaseConfig) {
	ContentSvc.dbConfig = dbConf
	ContentSvc.connect()
}

func (d *ContentService) connect() {

	if d.db != nil {
		return
	}

	var err error
	d.db, err = sql.Open("postgres", d.dbConfig.GetConnStr())
	if err != nil {
		fmt.Printf("open error %s, dsn: %s", err.Error(), d.dbConfig.GetConnStr())
		log.WithField("error", err.Error()).Fatal("db connect")
	}
	if err = d.db.Ping(); err != nil {
		fmt.Printf("ping error %s, dsn: %s", err.Error(), d.dbConfig.GetConnStr())
		log.WithField("error", err.Error()).Fatal("db ping")
	}

	d.db.SetMaxOpenConns(d.dbConfig.MaxOpenConns)
	d.db.SetConnMaxLifetime(time.Second * time.Duration(d.dbConfig.ConnMaxLifetime))

	log.WithFields(log.Fields{
		"host", d.dbConfig.Host, "dbname", d.dbConfig.Name, "user", d.dbConfig.User}).Debug("database connected")
	return
}
