package rpcclient

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	contentd_service "github.com/vostrok/contentd/service"
)

func init() {
	c := RPCClientConfig{
		DSN:     "localhost:50301",
		Timeout: 10,
	}
	Init(c)
}

func TestGetOperator(t *testing.T) {
	contentProperties, err := GetUniqueUrl(contentd_service.GetContentParams{
		Msisdn:       "79997777777",
		Tid:          "test tid",
		ServiceId:    777,
		CampaignId:   290,
		OperatorCode: 410,
		CountryCode:  92,
	})
	uniqUrl := contentProperties.UniqueUrl

	res, err := GetByUniqueUrl(uniqUrl)
	fmt.Printf("%#v %#v\n", res, err)
	assert.Nil(t, err)
	assert.Equal(t, res.UniqueUrl, uniqUrl)
}
