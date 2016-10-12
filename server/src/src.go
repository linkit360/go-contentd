package src

import (
	"net"
	"runtime"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/contrib/expvar"
	"github.com/gin-gonic/gin"
	"net/rpc"
	"net/rpc/jsonrpc"

	"github.com/vostrok/contentd/server/src/config"
	"github.com/vostrok/contentd/server/src/handlers"
	"github.com/vostrok/contentd/server/src/metrics"
	"github.com/vostrok/contentd/server/src/newrelic"
)

func Run() {
	appConfig := config.LoadConfig()
	metrics.Init()
	newrelic.Init(appConfig.NewRelic)

	nuCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(nuCPU)
	log.WithField("CPUCount", nuCPU)

	l, err := net.Listen("tcp", appConfig.Server.Port)
	if err != nil {
		log.Fatal("netListen ", err.Error())
	}
	server := rpc.NewServer()
	server.RegisterName("SVC", &handlers.RPCContentService{})

	for {
		if conn, err := l.Accept(); err == nil {
			go server.ServeCodec(jsonrpc.NewServerCodec(conn))
		} else {
			log.WithField("error", err.Error()).Error("Accept")
		}
	}

	r := gin.New()
	rg := r.Group("/debug")
	rg.GET("/vars", expvar.Handler())
	r.Run(":" + appConfig.Server.MetricsPort)
}
