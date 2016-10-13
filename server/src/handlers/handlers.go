package handlers

import (
	"fmt"

	"github.com/vostrok/contentd/server/src/metrics"
	"github.com/vostrok/contentd/service"
	"time"
)

type RPCContentService struct{}

type GetContentByCampaignIdRequest struct {
	Msisdn       string
	CampaignHash string
}
type GetContentByCampaignIdResponse struct {
	Msg service.MsgRecordContentSent
}

func (rpc *RPCContentService) GetContentByCampaign(req GetContentByCampaignIdRequest, res *GetContentByCampaignIdResponse) (err error) {
	begin := time.Now()
	defer func() {
		metrics.M.RequestsOverall.Time.CatchOverTime(time.Since(begin), time.Second)
	}()
	metrics.M.RequestsOverall.Count.Add(1)

	contentProperties, err := service.GetUrlByCampaignHash(req.Msisdn, req.CampaignHash)
	if err != nil {
		metrics.M.RequestsOverall.Errors.Add(1)
		return fmt.Errorf("GetContentByServiceId: %s", err.Error())
	}

	res.Msg = contentProperties
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
