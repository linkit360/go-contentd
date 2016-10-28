package handlers

import (
	"fmt"
	"time"

	"github.com/vostrok/contentd/server/src/metrics"
	"github.com/vostrok/contentd/service"
)

type RPCContentService struct{}

func (rpc *RPCContentService) GetContentByCampaign(
	req service.GetUrlByCampaignHashParams, res *service.ContentSentProperties) (err error) {

	begin := time.Now()
	defer func() {
		metrics.M.RequestsOverall.Time.CatchOverTime(time.Since(begin), time.Second)
	}()
	metrics.M.RequestsOverall.Count.Add(1)

	*res, err = service.GetUrlByCampaignHash(req)
	if err != nil {
		metrics.M.RequestsOverall.Errors.Add(1)
		return fmt.Errorf("GetContentByServiceId: %s", err.Error())
	}
	return nil
}

type CQRRequest struct {
	Table string `json:"table,omitempty"`
}
type CQRResponse struct {
	Success bool `json:"success,omitempty"`
}

func (rpc *RPCContentService) CQR(req CQRRequest, res *CQRResponse) (err error) {
	begin := time.Now()
	defer func() {
		metrics.M.CQRRequest.Time.CatchOverTime(time.Since(begin), time.Second*time.Duration(10))
	}()
	metrics.M.CQRRequest.Count.Add(1)

	success, err := service.CQR(req.Table)
	if err != nil {
		metrics.M.CQRRequest.Errors.Add(1)
		return fmt.Errorf("GetContentByServiceId: %s", err.Error())
	}
	res.Success = success
	return nil
}
