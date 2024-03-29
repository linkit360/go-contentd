package src

// server has metrics, config app
// and handles rpc method to get content url by campaign hash
// and another method to update cache on demand (CQR)
// anyway, there is a http method to catch metrics
import (
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"runtime"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"

	"github.com/linkit360/go-contentd/server/src/config"
	"github.com/linkit360/go-contentd/server/src/handlers"
	"github.com/linkit360/go-contentd/server/src/service"
	"github.com/linkit360/go-utils/metrics"
)

func Run() {
	appConfig := config.LoadConfig()

	service.InitService(
		appConfig.AppName,
		appConfig.Service,
		appConfig.MidConfig,
		appConfig.Notifier,
	)

	nuCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(nuCPU)
	log.WithField("CPUCount", nuCPU)

	go runGin(appConfig)
	runRPC(appConfig)
}

func runGin(appConfig config.AppConfig) {
	r := gin.New()
	metrics.AddHandler(r)

	r.Run(appConfig.Server.Host + ":" + appConfig.Server.HttpPort)
	log.WithField("port", appConfig.Server.HttpPort).Info("service port")
}

func runRPC(appConfig config.AppConfig) {

	l, err := net.Listen("tcp", appConfig.Server.Host+":"+appConfig.Server.RPCPort)
	if err != nil {
		log.Fatal("netListen ", err.Error())
	} else {
		log.WithField("port", appConfig.Server.RPCPort).Info("rpc port")
	}

	server := rpc.NewServer()
	server.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	server.RegisterName("SVC", &handlers.RPCContentService{})

	for {
		if conn, err := l.Accept(); err == nil {
			go server.ServeCodec(jsonrpc.NewServerCodec(conn))
		} else {
			log.WithField("error", err.Error()).Error("accept")
		}
	}
}
