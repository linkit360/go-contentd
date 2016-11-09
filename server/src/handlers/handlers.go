package handlers

import (
	"github.com/vostrok/contentd/service"
)

type RPCContentService struct{}

func (rpc *RPCContentService) GetContentByCampaign(
	req service.GetUrlByCampaignHashParams, res *service.ContentSentProperties) (err error) {

	*res, err = service.GetUrlByCampaignHash(req)
	if err != nil {
		res.Error = err.Error()
		err = nil
	}
	return nil
}
