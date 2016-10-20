package src

// server has metrics, config, newrelic app
// and handles rpc method to get content url by campaign hash
// and another method to update cache on demand (CQR)
// anyway, there is a http method to catch metrics
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
	"github.com/vostrok/contentd/service"
)

func Run() {
	appConfig := config.LoadConfig()
	metrics.Init()
	newrelic.Init(appConfig.NewRelic)
	service.InitService(appConfig.Service)

	nuCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(nuCPU)
	log.WithField("CPUCount", nuCPU)

	go runGin(appConfig)
	runRPC(appConfig)
}

func runGin(appConfig config.AppConfig) {
	r := gin.New()
	service.AddCQRHandler(r)

	rg := r.Group("/debug")
	rg.GET("/vars", expvar.Handler())

	r.Run(":" + appConfig.Server.MetricsPort)
	log.WithField("port", appConfig.Server.MetricsPort).Info("service port")
}

func runRPC(appConfig config.AppConfig) {
	l, err := net.Listen("tcp", ":"+appConfig.Server.Port)
	if err != nil {
		log.Fatal("netListen ", err.Error())
	} else {
		log.WithField("port", appConfig.Server.Port).Info("rpc port")
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
}
