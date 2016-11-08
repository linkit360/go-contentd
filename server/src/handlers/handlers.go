package handlers

import (
	"fmt"

	"github.com/vostrok/contentd/service"
)

type RPCContentService struct{}

func (rpc *RPCContentService) GetContentByCampaign(
	req service.GetUrlByCampaignHashParams, res *service.ContentSentProperties) (err error) {

	*res, err = service.GetUrlByCampaignHash(req)
	if err != nil {
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
	success, err := service.CQR(req.Table)
	if err != nil {
		return fmt.Errorf("GetContentByServiceId: %s", err.Error())
	}
	res.Success = success
	return nil
}
