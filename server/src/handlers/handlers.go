package handlers

import (
	log "github.com/Sirupsen/logrus"

	"github.com/linkit360/go-contentd/server/src/service"
	inmem_service "github.com/linkit360/go-mid/service"
)

// this is the json rpc server for the service

type RPCContentService struct{}

func (rpc *RPCContentService) GetContent(
	req service.GetContentParams, res *inmem_service.ContentSentProperties) (err error) {

	log.WithFields(log.Fields{}).Info("GetContent")

	*res, err = service.GetContent(req)
	if err != nil {
		res.Error = err.Error()
		err = nil
	}
	return nil
}

func (rpc *RPCContentService) GetUniqueUrl(
	req service.GetContentParams, res *inmem_service.ContentSentProperties) (err error) {

	log.WithFields(log.Fields{}).Info("GetUniqueUrl")

	contentProperties, err := service.GetContent(req)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("GetUniqueUrl")
		res.Error = err.Error()
		err = nil
		return
	}

	uniqueUrl, err := service.CreateUniqueUrl(contentProperties)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("GetUniqueUrl")
		res.Error = err.Error()
		err = nil
		return
	}
	contentProperties.UniqueUrl = uniqueUrl
	*res = contentProperties
	return nil
}

func (rpc *RPCContentService) GetByUniqueUrl(
	req string, res *inmem_service.ContentSentProperties) (err error) {

	log.WithFields(log.Fields{}).Info("GetByUniqueUrl")
	contentProperties, err := service.GetByUniqueUrl(req)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("GetByUniqueUrl")
		res.Error = err.Error()
		err = nil
		return
	}
	*res = contentProperties
	return nil
}
