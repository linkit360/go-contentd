package rpcclient

// rpc client for "github.com/vostrok/contentd/server"
// fails when disconnected

import (
	"fmt"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"time"

	log "github.com/sirupsen/logrus"

	content_service "github.com/linkit360/go-contentd/server/src/service"
	m "github.com/linkit360/go-utils/metrics"
	"github.com/linkit360/go-utils/structs"
)

var cli *Client

type Client struct {
	connection *rpc.Client
	conf       ClientConfig
	m          *Metrics
}
type ClientConfig struct {
	DSN     string `default:":50301" yaml:"dsn"`
	Timeout int    `default:"10" yaml:"timeout"`
}

type Metrics struct {
	RPCConnectError m.Gauge
	RPCSuccess      m.Gauge
}

func initMetrics() *Metrics {
	m := &Metrics{
		RPCConnectError: m.NewGauge("rpc", "contentd", "errors", "RPC call errors"),
		RPCSuccess:      m.NewGauge("rpc", "contentd", "success", "RPC call success"),
	}
	go func() {
		for range time.Tick(time.Minute) {
			m.RPCConnectError.Update()
			m.RPCSuccess.Update()
		}
	}()
	return m
}
func Init(clientConf ClientConfig) error {
	var err error
	cli = &Client{
		conf: clientConf,
		m:    initMetrics(),
	}
	if err = cli.dial(); err != nil {
		err = fmt.Errorf("cli.dial: %s", err.Error())
		log.WithField("error", err.Error()).Error("contentd rpc client unavialable")
		return err
	}
	log.WithField("conf", fmt.Sprintf("%#v", clientConf)).Info("contentd rpc client init done")

	return nil
}

func (c *Client) dial() error {
	if c.connection != nil {
	}

	conn, err := net.DialTimeout(
		"tcp",
		c.conf.DSN,
		time.Duration(c.conf.Timeout)*time.Second,
	)
	if err != nil {
		log.WithFields(log.Fields{
			"dsn":   c.conf.DSN,
			"error": err.Error(),
		}).Error("dialing contentd")
		return err
	}
	c.connection = jsonrpc.NewClient(conn)
	log.WithFields(log.Fields{
		"dsn": c.conf.DSN,
	}).Debug("dial contentd done")
	return nil
}

func call(funcName string, req interface{}, res interface{}) error {
	begin := time.Now()

	retryCount := 0
retry:
	if err := cli.connection.Call(funcName, req, &res); err != nil {
		cli.m.RPCConnectError.Inc()

		if err == rpc.ErrShutdown {

			if retryCount < 2 {
				retryCount = retryCount + 1
				cli.connection.Close()
				cli.dial()
				log.WithFields(log.Fields{
					"retry": retryCount,
					"error": err.Error(),
				}).Debug("retrying..")
				goto retry
			}

			log.WithFields(log.Fields{
				"func":  funcName,
				"error": err.Error(),
			}).Fatal("call")
		}

		log.WithFields(log.Fields{
			"func":  funcName,
			"error": err.Error(),
			"type":  fmt.Sprintf("%T", err),
		}).Error("call")
		return err
	}
	log.WithFields(log.Fields{
		"func": funcName,
		"took": time.Since(begin),
	}).Debug("rpccall")
	cli.m.RPCSuccess.Inc()
	return nil
}

func Get(req content_service.GetContentParams) (*structs.ContentSentProperties, error) {
	var res structs.ContentSentProperties
	err := call(
		"SVC.GetContent",
		req,
		&res,
	)
	return &res, err
}
func GetUniqueUrl(req content_service.GetContentParams) (*structs.ContentSentProperties, error) {
	var res structs.ContentSentProperties
	err := call(
		"SVC.GetUniqueUrl",
		req,
		&res,
	)
	return &res, err
}
func GetByUniqueUrl(req string) (*structs.ContentSentProperties, error) {
	var res structs.ContentSentProperties
	err := call(
		"SVC.GetByUniqueUrl",
		req,
		&res,
	)
	return &res, err
}
