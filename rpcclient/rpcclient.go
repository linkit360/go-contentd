package rpcclient

// rpc client for "github.com/vostrok/contentd/server"
// supports reconnects when disconnected

import (
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/felixge/tcpkeepalive"

	"github.com/vostrok/contentd/service"
)

var contentClient *Client

type Client struct {
	connection *rpc.Client
	conf       RPCClientConfig
}
type RPCClientConfig struct {
	DSN     string `default:"localhost:70401" yaml:"dsn"`
	Timeout int    `default:"10" yaml:"timeout"`
}

func Init(contentdClientConf RPCClientConfig) {
	var err error
	contentClient = &Client{
		conf: contentdClientConf,
	}
	if err = contentClient.dial(); err != nil {
		log.WithField("error", err.Error()).Error("contentd rpc client unavialable")
		return
	}
}

func (c *Client) dial() error {
	if c.connection != nil {
		_ = c.connection.Close()
	}

	conn, err := net.DialTimeout("tcp", c.conf.DSN, time.Duration(c.conf.Timeout)*time.Second)
	if err != nil {
		return err
	}
	kaConn, _ := tcpkeepalive.EnableKeepAlive(conn)
	kaConn.SetKeepAliveIdle(30 * time.Second)
	kaConn.SetKeepAliveCount(4)
	kaConn.SetKeepAliveInterval(5 * time.Second)
	c.connection = jsonrpc.NewClient(kaConn)
	return nil
}

func Get(req service.GetContentParams) (*service.ContentSentProperties, error) {
	var res service.ContentSentProperties

	redialed := false
	if contentClient.connection == nil {
		contentClient.dial()
	}
redo:
	if err := contentClient.connection.Call("SVC.GetContent", req, &res); err != nil {
		log.WithFields(log.Fields{
			"tid":    req.Tid,
			"msg":    err.Error(),
			"msisdn": req.Msisdn,
		}).Debug("contentd rpc client now is unavialable")
		if !redialed {
			contentClient.dial()
			redialed = true
			goto redo
		}
		log.WithFields(log.Fields{
			"tid":    req.Tid,
			"error":  err.Error(),
			"msisdn": req.Msisdn,
		}).Error("redial did't help")
		return nil, err
	}

	return &res, nil
}

func GetUniqueUrl(req service.GetUniqueUrlParams) (*service.ContentSentProperties, error) {

	var res string
	redialed := false
	if contentClient.connection == nil {
		contentClient.dial()
	}
redo:
	if err := contentClient.connection.Call("SVC.GetUniqueUrl", req, &res); err != nil {
		log.WithFields(log.Fields{
			"msg":    err.Error(),
			"msisdn": req.Msisdn,
		}).Debug("contentd rpc client now is unavialable")
		if !redialed {
			contentClient.dial()
			redialed = true
			goto redo
		}
		log.WithFields(log.Fields{
			"campaign_id": req.CampaignId,
			"msisdn":      req.Msisdn,
			"error":       err.Error(),
		}).Error("redial did't help")
		return res, err
	}
	return res, nil
}

func GetByUniqueUrl(req string) (*service.ContentSentProperties, error) {

	var res string
	redialed := false
	if contentClient.connection == nil {
		contentClient.dial()
	}
redo:
	if err := contentClient.connection.Call("SVC.GetByUniqueUrl", req, &res); err != nil {
		log.WithFields(log.Fields{
			"msg": err.Error(),
		}).Debug("contentd rpc client now is unavialable")
		if !redialed {
			contentClient.dial()
			redialed = true
			goto redo
		}
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("redial did't help")
		return res, err
	}
	return res, nil
}
