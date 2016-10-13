package rpcclient

// rpc client for "github.com/vostrok/contentd/server"
import (
	"net"
	"time"

	"net/rpc"
	"net/rpc/jsonrpc"

	"github.com/vostrok/contentd/server/src/handlers"
	"github.com/vostrok/contentd/service"
)

type Client struct {
	connection *rpc.Client
	conf       RPCClientConfig
}
type RPCClientConfig struct {
	DSN     string `default:"localhost:70401" yaml:"dsn"`
	Timeout int    `default:"10" yaml:"timeout"`
}

func NewClient(dsn string, timeoutSeconds int) (*Client, error) {
	connection, err := net.DialTimeout("tcp", dsn, time.Duration(timeoutSeconds)*time.Second)
	if err != nil {
		return nil, err
	}
	return &Client{
		connection: jsonrpc.NewClient(connection),
		conf:       RPCClientConfig{DSN: dsn, Timeout: timeoutSeconds},
	}, nil
}

func (c *Client) Get(req service.GetUrlByCampaignHashParams) (msg service.ContentSentProperties, err error) {

	res := service.ContentSentProperties{}
	if err = c.connection.Call("SVC.GetContentByCampaign", req, &res); err != nil {
		return msg, err
	}
	return res, err
}

func (c *Client) CQR(table string) (success bool, err error) {

	req := handlers.CQRRequest{Table: table}
	res := handlers.CQRResponse{}
	if err = c.connection.Call("SVC.CQR", req, &res); err != nil {
		return false, err
	}
	return res.Success, err
}
