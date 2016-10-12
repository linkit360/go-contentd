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
	ContentPath string
}

func (rpc *RPCContentService) GetContentByCampaign(req GetContentByCampaignIdRequest, res *GetContentByCampaignIdResponse) (err error) {
	begin := time.Now()
	defer func() {
		metrics.M.RequestsOverall.Time.CatchOverTime(time.Since(begin), time.Second)
	}()
	metrics.M.RequestsOverall.Count.Add(1)

	contentUrl, err := service.GetContentByCampaignHash(req.Msisdn, req.CampaignHash)
	if err != nil {
		metrics.M.RequestsOverall.Errors.Add(1)
		return fmt.Errorf("GetContentByServiceId: %s", err.Error())
	}

	res.ContentPath = contentUrl
	return nil
}
