package content

import (
	"errors"

	"fmt"

	"github.com/vostrok/contentd/service"
)

func InitPackage(conf service.ContentServiceConfig) {
	service.InitService(conf)

}
func GetContentByCampaignId(msisdn, campaignHash string, countryCode, opCode int) (s string, err error) {
	contentUrl, err := service.GetContentByCampaignHash(msisdn, campaignHash, countryCode, opCode)
	if err != nil {
		return "", fmt.Errorf("GetContentByServiceId: %s", err.Error())
	}
	return contentUrl, errors.New("Not implemented")
}
