package handlers

import (
	"github.com/vostrok/contentd/service"
)

type RPCContentService struct{}

func (rpc *RPCContentService) GetContent(
	req service.GetContentParams, res *service.ContentSentProperties) (err error) {

	*res, err = service.GetContent(req)
	if err != nil {
		res.Error = err.Error()
		err = nil
	}
	return nil
}

func (rpc *RPCContentService) GetUniqueUrl(
	req service.GetContentParams, res *service.ContentSentProperties) (err error) {

	contentProperties, err := service.GetContent(req)
	if err != nil {
		res.Error = err.Error()
		err = nil
		return
	}

	uniqueUrl, err := service.CreateUniqueUrl(contentProperties)
	if err != nil {
		res.Error = err.Error()
		err = nil
		return
	}
	contentProperties.UniqueUrl = uniqueUrl
	*res = contentProperties
	return nil
}

func (rpc *RPCContentService) GetByUniqueUrl(
	req string, res *service.ContentSentProperties) (err error) {

	contentProperties, err := service.GetByUniqueUrl(req)
	if err != nil {
		res.Error = err.Error()
		err = nil
		return
	}
	*res = contentProperties
	return nil
}
