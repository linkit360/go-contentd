// this file makes 2 things:
// 1) - get content url by campaign hash and msisdn
// 2) - add a record to the content_sent table
// the file itself is big enough, bcz we keep all required tables in memory (as a in-memory cache)

package service

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	log "github.com/Sirupsen/logrus"
)

const ACTIVE_STATUS = 1

var ContentSvc ContentService

type ContentInterface interface {
	InitService(sConf ContentServiceConfig)

	GetUrlByCampaignHash(msisdn, campaignHash string) (path string, err error)
}

func InitService(sConf ContentServiceConfig) {
	initDatabase(sConf.dbConf)

	ContentSvc.sConfig = sConf
	ContentSvc.notifier = NewNotifierService(sConf.Notifier)
	if err := initCQR(); err != nil {
		log.WithField("error", err.Error()).Fatal("INit CQR")
	}
}

type ContentService struct {
	db       *sql.DB
	dbConfig DataBaseConfig
	sConfig  ContentServiceConfig
	notifier Notifier
	tables   map[string]struct{}
}
type ContentServiceConfig struct {
	dbConf           DataBaseConfig `yaml:"db"`
	Notifier         NotifierConfig `notifier:"notifier"`
	SearchRetryCount int            `default:"10" yaml:"retry_count"`
	TablePrefix      string         `default:"xmp_" yaml:"table_prefix"`
	UniqDays         int            `default:"10" yaml:"uniq_days"` // content would be uniq in these days
	Tables           []string       `default:"campaign,service,content,service_content,subscriptions,content_sent" yaml:"tables"`
}

func initCQR() error {
	if err := campaign.Reload(); err != nil {
		return fmt.Errorf("campaign.Reload: %s", err.Error())
	}
	if err := service.Reload(); err != nil {
		return fmt.Errorf("service.Reload: %s", err.Error())
	}
	if err := content.Reload(); err != nil {
		return fmt.Errorf("content.Reload: %s", err.Error())
	}
	if err := subscriptions.Reload(); err != nil {
		return fmt.Errorf("subscriptions.Reload: %s", err.Error())
	}
	if err := contentSent.Reload(); err != nil {
		return fmt.Errorf("sentContent.Reload: %s", err.Error())
	}
	ContentSvc.tables = make(map[string]struct{}, len(ContentSvc.sConfig.Tables))
	for _, v := range ContentSvc.sConfig.Tables {
		ContentSvc.tables[v] = struct{}{}
	}
	return nil
}

func CQR(table string) (bool, error) {
	if len(table) == 0 {
		log.WithField("error", "No table name given").Errorf("CQR request")
		return false, nil
	}
	_, ok := ContentSvc.tables[table]
	if !ok {
		log.WithField("error", "table name doesn't match any").Errorf("CQR request")
		return false, nil
	}
	// should we re-build content_service
	switch table {
	case "campaign":
		if err := campaign.Reload(); err != nil {
			return false, fmt.Errorf("campaign.Reload: %s", err.Error())
		}
	case "service":
		if err := service.Reload(); err != nil {
			return false, fmt.Errorf("service.Reload: %s", err.Error())
		}
	case "content":
		if err := content.Reload(); err != nil {
			return false, fmt.Errorf("content.Reload: %s", err.Error())
		}
	case "service_content":
		if err := service.Reload(); err != nil {
			return false, fmt.Errorf("service_content: service.Reload: %s", err.Error())
		}
	case "subscriptions":
		if err := subscriptions.Reload(); err != nil {
			return false, fmt.Errorf("subscriptions.Reload: %s", err.Error())
		}
	case "content_sent":
		if err := contentSent.Reload(); err != nil {
			return false, fmt.Errorf("sentContent.Reload: %s", err.Error())
		}
	default:
		return false, fmt.Errorf("CQR Request: Unknown table: %s", table)
	}

	return true, nil
}

type GetUrlByCampaignHashParams struct {
	Msisdn       string
	CampaignHash string
	Tid          string
	CountryCode  int64
	OperatorCode int64
}

// Get Content by campaign hash
// 1) find first avialable contentId
// 2) reset cache if nothing found
// 3) record that the content is shown to the user
func GetUrlByCampaignHash(p GetUrlByCampaignHashParams) (msg *ContentSentProperties, err error) {
	logCtx := log.WithFields(log.Fields{
		"msisdn":       p.Msisdn,
		"campaignHash": p.CampaignHash,
		"CountryCode":  p.CountryCode,
		"OperatorCode": p.OperatorCode,
		"Tid":          p.Tid,
	})
	if p.Msisdn == "" || p.CampaignHash == "" || p.CountryCode == 0 || p.OperatorCode == 0 {
		err = errors.New("Empty required params")
		logCtx.WithField("error", err.Error()).Errorf("No required params")
	}
	campaign, ok := campaign.Map[p.CampaignHash]
	if !ok {
		err = errors.New("Not found")
		logCtx.WithField("error", err.Error()).Errorf("get campaign by campaign hash")
		return msg, fmt.Errorf("CampaignHash: %s", "Not found")
	}

	serviceId := campaign.ServiceId
	usedContentIds := contentSent.Get(p.Msisdn, serviceId)
	logCtx.WithField("usedContentIds", usedContentIds)

	avialableContentIds := service.Get(serviceId)
	logCtx.WithField("avialableContentIds", avialableContentIds)
	if len(avialableContentIds) == 0 {
		err = fmt.Errorf("No content for campaign %s at all", p.CampaignHash)
		logCtx.WithField("error", err.Error()).Errorf("No content avialabale at all")
		return msg, err
	}

	// todo:
	// Platform compatibility check
	// Subcategory Check
	//
	//switch {
	//case strings.Contains(os, "Windows"):
	//	platform_id = 3
	//	break
	//case strings.Contains(os, "Android"):
	//	platform_id = 1
	//	break
	//case strings.Contains(os, "iOS"):
	//	platform_id = 2
	//	break
	//case strings.Contains(os, "BlackBerry"):
	//	platform_id = 4
	//	break
	//case strings.Contains(os, "Symbian"):
	//	platform_id = 14
	//	break
	//default:
	//	platform_id = 0
	//}
	//
	//if platform_id != content.PlatformId && content.PlatformId != 0 {
	//	//----------------------- Search All Active Content by Platform -------------------------
	//	content_records, err := database.SearchAllPlatformContent(platform_id, 1)
	//	if err != nil {
	//		return nil, errors.NotFound("go.micro.srv.content.Deliver.SearchPlatformContent", err.Error())
	//	}
	//
	//	if len(content_records) == 0 {
	//		return nil, errors.NotFound("go.micro.srv.content.Deliver.SearchPlatformContent", "no compatibile platform")
	//	}
	//
	//	//----------------------- Search in SubCategory -------------------------
	//	for _, content_record := range content_records {
	//		if content.SubCategoryId == content_record.SubCategoryId {
	//			return content_record, nil
	//		}
	//	}
	//
	//	//----------------------- Search in Category -------------------------
	//	for _, content_record := range content_records {
	//		if content.CategoryId == content_record.CategoryId {
	//			return content_record, nil
	//		}
	//	}
	//
	//	//----------------------- Search in Publishers -------------------------
	//	for _, content_record := range content_records {
	//		if content.PublisherId == content_record.PublisherId {
	//			return content_record, nil
	//		}
	//	}
	//
	//	//----------------------- Random content -------------------------
	//	content = content_records[rand.Intn(len(content_records)-1)]
	//}

	retry := 0
findContentId:
	// find first avialable contentId
	contentId := int64(0)
	for id, _ := range avialableContentIds {
		if usedContentIds != nil {
			if _, ok := usedContentIds[int64(id)]; ok {
				continue
			}
		}
		contentId = int64(id)
		break
	}
	// reset if nothing
	if contentId == 0 {
		logCtx.Debug("No content avialable, reset remembered cache..")
		contentSent.Clear(p.Msisdn, serviceId)
		for id, _ := range avialableContentIds {
			contentId = int64(id)
			break
		}

	}
	// update in-memory cache usedContentIds
	contentSent.Push(p.Msisdn, serviceId, contentId)
	logCtx.WithField("ContentId", contentId).Debug("choosen content")

	path, ok := content.Map[contentId]
	if !ok {
		if retry < ContentSvc.sConfig.SearchRetryCount {
			retry++
			logCtx.WithFields(log.Fields{"ContentId": contentId, "Retry": retry}).
				Error("Service ContentId not found in content")
			goto findContentId
		} else {
			err = fmt.Errorf("Failed to find valid contentId: campaign: %s, msisdn: %s", p.CampaignHash, p.Msisdn)
			logCtx.WithFields(log.Fields{"ContentId": contentId, "Retry": retry}).
				Error(err.Error())
			return msg, err
		}
	}

	// XXX: dispatcher needs path only,
	// and, as we got this variables during "get path" process
	// we save them into database, not to get them again
	// anyway, it is possible to find a better way in future
	msg = &ContentSentProperties{
		Msisdn:       p.Msisdn,
		Tid:          p.Tid,
		ContentPath:  path,
		CampaignId:   campaign.Id,
		ContentId:    contentId,
		ServiceId:    serviceId,
		CountryCode:  p.CountryCode,
		OperatorCode: p.OperatorCode,
	}
	// record sent content
	ContentSvc.notifier.ContentSentNotify(msg)

	return msg, nil
}

// Tasks:
// Keep in memory all active service to content mapping
// Allow to get all content ids of given service id
// Reload when changes to service_content or service are done
var service = &Services{}

type Services struct {
	sync.RWMutex
	Map map[int64][]int64
}
type Service struct {
	Id int64
}
type Content struct {
	Id     int64
	Object string
}
type ServiceContent struct {
	IdService int64
	IdContent int64
}

func (s Services) Reload() error {
	query := fmt.Sprintf("select id from %sservices where status = $1", ContentSvc.sConfig.TablePrefix)
	rows, err := ContentSvc.db.Query(query, ACTIVE_STATUS)
	if err != nil {
		return fmt.Errorf("services QueryServices: %s, query: %s", err.Error(), query)
	}
	defer rows.Close()

	var serviceIds []int64
	for rows.Next() {
		var serviceId int64
		if err := rows.Scan(
			&serviceId,
		); err != nil {
			return err
		}
		serviceIds = append(serviceIds, serviceId)
	}
	if rows.Err() != nil {
		return fmt.Errorf("RowsError: %s", err.Error())
	}

	placeHoldersForServiceIds := []string{}
	startIdx := 2
	for range serviceIds {
		placeHoldersForServiceIds = append(placeHoldersForServiceIds, "$"+strconv.Itoa(startIdx))
		startIdx++
	}
	query = fmt.Sprintf("select id_service, id_content from %sservice_content where status = $1 and id_service IN (%s)",
		ContentSvc.sConfig.TablePrefix, strings.Join(placeHoldersForServiceIds, ", "))
	rows, err = ContentSvc.db.Query(query, ACTIVE_STATUS, serviceIds)
	if err != nil {
		return fmt.Errorf("service_content QueryServices: %s, query: %s", err.Error(), query)
	}
	defer rows.Close()

	var serviceMap []ServiceContent
	for rows.Next() {
		var serviceContent ServiceContent
		if err := rows.Scan(
			&serviceContent.IdService,
			&serviceContent.IdContent,
		); err != nil {
			return err
		}
		serviceMap = append(serviceMap, serviceContent)
	}
	if rows.Err() != nil {
		return fmt.Errorf("service_content RowsError: %s", err.Error())
	}

	s.Lock()
	defer s.Unlock()

	s.Map = make(map[int64][]int64)
	for _, service := range serviceMap {
		if _, ok := s.Map[service.IdService]; !ok {
			s.Map[service.IdService] = []int64{}
		}
		s.Map[service.IdService] = append(s.Map[service.IdService], service.IdContent)
	}
	return nil
}

func (s Services) Get(serviceId int64) (contentIds []int64) {
	contentIds, _ = s.Map[serviceId]
	return
}

// Tasks:
// Keep in memory all active content_ids mapping to their object string (url path to content)
// Allow to get object for given content id
// Reload when changes to content
var content = &Contents{}

type Contents struct {
	sync.RWMutex
	Map map[int64]string
}

func (s Contents) Reload() error {
	query := fmt.Sprintf("select id from %scontent where status = $1", ContentSvc.sConfig.TablePrefix)
	rows, err := ContentSvc.db.Query(query, ACTIVE_STATUS)
	if err != nil {
		return fmt.Errorf("content QueryServices: %s, query: %s", err.Error(), query)
	}
	defer rows.Close()

	var contents []Content
	for rows.Next() {
		var c Content
		if err := rows.Scan(&c.Id, &c.Object); err != nil {
			return err
		}
		contents = append(contents, c)
	}
	if rows.Err() != nil {
		return fmt.Errorf("RowsError: %s", err.Error())
	}

	s.Lock()
	defer s.Unlock()

	s.Map = make(map[int64]string)
	for _, content := range contents {
		s.Map[content.Id] = content.Object
	}
	return nil
}

// Tasks:
// Keep in memory all active campaigns
// Allow to get a service_id by campaign hash fastly
// Reload when changes to campaigns are done
var campaign = &Campaigns{}

type Campaigns struct {
	sync.RWMutex
	Map map[string]Campaign
}
type Campaign struct {
	Hash      string
	Id        int64
	ServiceId int64
}

func (s Campaigns) Reload() error {
	query := fmt.Sprintf("select id, hash, service_id_1 from %scampaigns where status = $1",
		ContentSvc.sConfig.TablePrefix)
	rows, err := ContentSvc.db.Query(query, ACTIVE_STATUS)
	if err != nil {
		return fmt.Errorf("QueryServices: %s, query: %s", err.Error(), query)
	}
	defer rows.Close()

	var records []Campaign
	for rows.Next() {
		record := Campaign{}

		if err := rows.Scan(
			&record.Id,
			&record.Hash,
			&record.ServiceId,
		); err != nil {
			return err
		}
		records = append(records, record)
	}
	if rows.Err() != nil {
		return fmt.Errorf("RowsError: %s", err.Error())
	}

	s.Lock()
	defer s.Unlock()

	s.Map = make(map[string]Campaign, len(records))
	for _, campaign := range records {
		s.Map[campaign.Hash] = campaign
	}
	return nil
}

// msisdn _ service_id ==> subscription_id

// Keep in memory all active campaigns
// Allow to get a subscription_id by msisdn and service_id
// Reload when changes to subscriptions made
// usage:
// subscripions_id := subscripions.Map[ s.key() ]
var subscriptions = &Subscriptions{}

type Subscriptions struct {
	sync.RWMutex
	Map map[string]int64
}
type Subscription struct {
	Msisdn         string
	ServiceId      int64
	SubscriptionId int64
}

func (s Subscription) key() string {
	return fmt.Sprintf("%s-%d", s.Msisdn, s.ServiceId)
}
func (s Subscriptions) Reload() error {
	query := fmt.Sprintf("select msisdn, id_service, id_subscription from "+
		"%ssubscriptions where status = $1", ContentSvc.sConfig.TablePrefix)
	rows, err := ContentSvc.db.Query(query, ACTIVE_STATUS)
	if err != nil {
		return fmt.Errorf("Subscriptions Query: %s, query: %s", err.Error(), query)
	}
	defer rows.Close()

	var records []Subscription
	for rows.Next() {
		record := Subscription{}

		if err := rows.Scan(
			&record.Msisdn,
			&record.ServiceId,
			&record.SubscriptionId,
		); err != nil {
			return err
		}
		records = append(records, record)
	}
	if rows.Err() != nil {
		return fmt.Errorf("Subscriptions Reload RowsError: %s", err.Error())
	}

	s.Lock()
	defer s.Unlock()

	s.Map = make(map[string]int64, len(records))
	for _, subscription := range records {
		s.Map[subscription.key()] = subscription.SubscriptionId
	}
	return nil
}

var contentSent = &SentContents{}

// When updating from database, reading is forbidden
// Map structure: map [ msisdn + service_id ] []content_id
// where
// * msisdn + service_id -- is a sentCOntent key (see below) (could be changed to msisdn)
// * content_id is content that was shown to msisdn
type SentContents struct {
	sync.RWMutex
	Map map[string]map[int64]struct{}
}

// sent content Data that neded to build in-memory cache of used content-ids
// and alos need for recording "got content"
type ContentSentProperties struct {
	Msisdn         string `json:"msisdn"`
	Tid            string `json:"tid"`
	ContentPath    string `json:"content_path"`
	CapmaignHash   string `json:"capmaign_hash"`
	CampaignId     int64  `json:"campaign_id"`
	ContentId      int64  `json:"content_id"`
	ServiceId      int64  `json:"service_id"`
	SubscriptionId int64  `json:"subscription_id"`
	CountryCode    int64  `json:"country_code"`
	OperatorCode   int64  `json:"operator_code"`
}

// Used to get a key of used content ids
// when key == msisdn, then uniq content exactly
// when key == msisdn + service+id, then unique content per sevice
func (t ContentSentProperties) key() string {
	return t.Msisdn + "-" + strconv.FormatInt(t.ServiceId, 10)
}

// Load sent contents to filter content that had been seen by the msisdn.
// created at == before date specified in config
func (s SentContents) Reload() error {
	query := fmt.Sprintf("select msisdn, id_service, id_content "+
		"from %scontent_sent "+
		"where created_at > (CURRENT_TIMESTAMP - INTERVAL '$3 days')",
		ContentSvc.sConfig.TablePrefix,
	)
	rows, err := ContentSvc.db.Query(query, ContentSvc.sConfig.UniqDays)

	if err != nil {
		return fmt.Errorf("SentContent Reload QueryServices: %s, query: %s", err.Error(), query)
	}
	defer rows.Close()

	var records []ContentSentProperties
	for rows.Next() {
		record := ContentSentProperties{}

		if err := rows.Scan(
			&record.Msisdn,
			&record.ServiceId,
			&record.ContentId,
		); err != nil {
			return err
		}
		records = append(records, record)
	}
	if rows.Err() != nil {
		return fmt.Errorf("ContentSent Reload RowsError: %s", err.Error())
	}

	s.Lock()
	defer s.Unlock()

	s.Map = make(map[string]map[int64]struct{})
	for _, sentContent := range records {
		if _, ok := s.Map[sentContent.key()]; !ok {
			s.Map[sentContent.key()] = make(map[int64]struct{})
		}
		s.Map[sentContent.key()][sentContent.ContentId] = struct{}{}
	}
	return nil
}

// Get content ids that was seen by msisdn
// Attention: filtered by service id also,
// so if we would have had content id on one service and the same content id on another service as a content id
// then it had used as different contens! And will shown
func (s SentContents) Get(msisdn string, serviceId int64) (contentIds map[int64]struct{}) {
	var ok bool
	t := ContentSentProperties{Msisdn: msisdn, ServiceId: serviceId}
	if contentIds, ok = s.Map[t.key()]; ok {
		return contentIds
	}
	return nil
}

// When there is no content avialabe for the msisdn, reset the content counter
// Breakes after reloading sent content table (on the restart of the application)
func (s SentContents) Clear(msisdn string, serviceId int64) {
	t := ContentSentProperties{Msisdn: msisdn, ServiceId: serviceId}
	delete(s.Map, t.key())
}

// After we have chosen the content to show,
// we notice it in sent content table (another place)
// and also we need to update in-memory cache of used content id for this msisdn and service id
func (s SentContents) Push(msisdn string, serviceId int64, contentId int64) {
	t := ContentSentProperties{Msisdn: msisdn, ServiceId: serviceId}
	if _, ok := s.Map[t.key()]; !ok {
		s.Map[t.key()] = make(map[int64]struct{})
	}
	s.Map[t.key()][contentId] = struct{}{}
}
