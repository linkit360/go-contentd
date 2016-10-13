// this file makes 2 things:
// 1) - get content url by campaign hash and msisdn
// 2) - add a record to the content_sent table
// the file itself is big enough, bcz we keep all required tables in memory (as a in-memory cache)

package service

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

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
	ContentSvc.recordContentGiven = make(chan MsgRecordContentSent)
	if err := initCQR(); err != nil {
		log.WithField("error", err.Error()).Fatal("INit CQR")
	}
	go func() {
		recordContentGiven()
	}()
}

type ContentService struct {
	db                 *sql.DB
	dbConfig           DataBaseConfig
	sConfig            ContentServiceConfig
	tables             map[string]struct{}
	recordContentGiven chan MsgRecordContentSent
}
type ContentServiceConfig struct {
	dbConf           DataBaseConfig `yaml:"db"`
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

// Get Content by campaign hash
// 1) find first avialable contentId
// 2) reset cache if nothing found
// 3) record that the content is shown to the user
func GetUrlByCampaignHash(msisdn, campaignHash string) (msg MsgRecordContentSent, err error) {
	logCtx := log.WithFields(log.Fields{"msisdn": msisdn, "campaignHash": campaignHash})

	campaign, ok := campaign.Map[campaignHash]
	if !ok {
		return msg, fmt.Errorf("CampaignHash: %s", "Not found")
	}

	serviceId := campaign.ServiceId
	usedContentIds := contentSent.Get(msisdn, serviceId)
	logCtx.WithField("usedContentIds", usedContentIds)

	avialableContentIds := service.Get(serviceId)
	logCtx.WithField("avialableContentIds", avialableContentIds)
	if len(avialableContentIds) == 0 {
		err = fmt.Errorf("No content for campaign %s at all", campaignHash)
		logCtx.WithField("error", err.Error()).Errorf("No content avialabale")
		return msg, err
	}

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
		contentSent.Clear(msisdn, serviceId)
		for id, _ := range avialableContentIds {
			contentId = int64(id)
			break
		}

	}
	// update in-memory cache usedContentIds
	contentSent.Push(msisdn, serviceId, contentId)
	logCtx.WithField("ContentId", contentId).Debug("choosen content")

	path, ok := content.Map[contentId]
	if !ok {
		if retry < ContentSvc.sConfig.SearchRetryCount {
			retry++
			logCtx.WithFields(log.Fields{"ContentId": contentId, "Retry": retry}).
				Error("Service ContentId not found in content")
			goto findContentId
		} else {
			err = fmt.Errorf("Failed to find valid contentId: campaign: %s, msisdn: %s", campaignHash, msisdn)
			logCtx.WithFields(log.Fields{"ContentId": contentId, "Retry": retry}).
				Error(err.Error())
			return msg, err
		}
	}

	s := Subscription{Msisdn: msisdn, ServiceId: serviceId}
	subscriptionId, ok := subscriptions.Map[s.key()]
	if !ok {
		return msg, fmt.Errorf("Get subscription: %s", "Not found")
	}

	msg = MsgRecordContentSent{
		Msisdn:         msisdn,
		ContentPath:    path,
		CampaignId:     campaign.Id,
		ContentId:      contentId,
		ServiceId:      serviceId,
		SubscriptionId: subscriptionId,
	}
	// record sent content
	ContentSvc.recordContentGiven <- msg

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
	query := fmt.Sprintf("select id, hash, service_id_1 from %scampaigns where status = $1", ContentSvc.sConfig.TablePrefix)
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
type MsgRecordContentSent struct {
	Msisdn         string `json:"msisdn"`
	ContentPath    string `json:"content_path"`
	CapmaignHash   string `json:"capmaign_hash"`
	CampaignId     int64  `json:"campaign_id"`
	ContentId      int64  `json:"content_id"`
	ServiceId      int64  `json:"service_id"`
	SubscriptionId int64  `json:"subscription_id"`
}

// Used to get a key of used content ids
// when key == msisdn, then uniq content exactly
// when key == msisdn + service+id, then unique content per sevice
func (t MsgRecordContentSent) key() string {
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

	var records []MsgRecordContentSent
	for rows.Next() {
		record := MsgRecordContentSent{}

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
	t := MsgRecordContentSent{Msisdn: msisdn, ServiceId: serviceId}
	if contentIds, ok = s.Map[t.key()]; ok {
		return contentIds
	}
	return nil
}

// When there is no content avialabe for the msisdn, reset the content counter
// Breakes after reloading sent content table (on the restart of the application)
func (s SentContents) Clear(msisdn string, serviceId int64) {
	t := MsgRecordContentSent{Msisdn: msisdn, ServiceId: serviceId}
	delete(s.Map, t.key())
}

// After we have chosen the content to show,
// we notice it in sent content table (another place)
// and also we need to update in-memory cache of used content id for this msisdn and service id
func (s SentContents) Push(msisdn string, serviceId int64, contentId int64) {
	t := MsgRecordContentSent{Msisdn: msisdn, ServiceId: serviceId}
	if _, ok := s.Map[t.key()]; !ok {
		s.Map[t.key()] = make(map[int64]struct{})
	}
	s.Map[t.key()][contentId] = struct{}{}
}

// Read from channel and insert records into the database.
// In case of any error, re-put message of content_sent into the channel
func recordContentGiven() {

	go func() {
		for {
			var t MsgRecordContentSent
			t = <-ContentSvc.recordContentGiven

			query := "INSERT INTO %scontent_sent" +
				" (msisdn, id_campaign, id_service, id_subscription, id_content) values ($1, $2, $3, $4, $5)"

			_, err := ContentSvc.db.Exec(query,
				t.Msisdn, t.CampaignId, t.ServiceId, t.SubscriptionId, t.ContentId)

			if err != nil {
				// we assume that the query and data are ok
				// so if database is buzy, whe wait until it ok
				log.WithFields(log.Fields{"sentContent": t, "error": err.Error()}).Error("add sent content")
				ContentSvc.recordContentGiven <- t
				time.Sleep(time.Second)
			}
		}
	}()
}
