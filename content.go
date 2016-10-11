package content

import (
	"errors"

	"fmt"
	"github.com/vostrok/contentd/db"
)

func InitPackage(conf db.DataBaseConfig) {
	db.InitDatabase(conf)

}
func GetContentByCampaignId(msisdn, campaignHash string) (s string, err error) {
	contentUrl, err := db.GetContentByCampaignHash(msisdn, campaignHash)
	if err != nil {
		return "", fmt.Errorf("GetContentByServiceId: %s", err.Error())
	}
	return contentUrl, errors.New("Not implemented")
}
