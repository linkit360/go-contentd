package service

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"

	log "github.com/Sirupsen/logrus"
)

// TODO - add CQRs
const ACTIVE_STATUS = 1

var ContentSvc ContentService

type ContentInterface interface {
	InitService(sConf ContentServiceConfig)

	GetContentByCampaignHash(msisdn, campaignHash string) (path string, err error)
}

func InitService(sConf ContentServiceConfig) {
	initDatabase(sConf.dbConf)
	ContentSvc.sConfig = sConf
	ContentSvc.recordContentGiven = make(chan MsgRecordContent)

	go func() {
		recordContentGiven()
	}()
}

type ContentService struct {
	db                 *sql.DB
	dbConfig           DataBaseConfig
	sConfig            ContentServiceConfig
	recordContentGiven chan MsgRecordContent
}
type ContentServiceConfig struct {
	dbConf           DataBaseConfig `yaml:"db"`
	SearchRetryCount int            `default:"10" yaml:"retry_count"`
	TablePrefix      string         `default:"xmp_" yaml:"table_prefix"`
	TransactionType  int            `default:"1" yaml:"transaction_type"`
	UniqDays         int            `default:"10" yaml:"uniq_days"` // content would be uniq in these days
}
type MsgRecordContent struct {
	CapmaignHash   string `json:"capmaign_hash"`
	CampaignId     int64  `json:"campaign_id"`
	Msisdn         string `json:"msisdn"`
	ContentId      int64  `json:"content_id"`
	ServiceId      int64  `json:"service_id"`
	SubscriptionId int64  `json:"subscription_id"`
}

type CQRRequest struct {
	Table string `json:"table,omitempty"`
}

func InitCQR() error {
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
	if err := transactions.Reload(); err != nil {
		return fmt.Errorf("transactions.Reload: %s", err.Error())
	}
	return nil
}

// Get Content by campaign hash
// Also, record transaction: content was shown to the user
// 1) find first avialable contentId
// 2) reset cache if nothing found
// 3) record transaction
func GetContentByCampaignHash(msisdn, campaignHash string) (path string, err error) {
	logCtx := log.WithFields(log.Fields{"msisdn": msisdn, "campaignHash": campaignHash})

	campaign, ok := campaign.Map[campaignHash]
	if !ok {
		return "", fmt.Errorf("CampaignHash: %s", "Not found")
	}
	serviceId := campaign.ServiceId
	usedContentIds := transactions.Get(msisdn, serviceId)
	logCtx.WithField("usedContentIds", usedContentIds)

	avialableContentIds := service.Get(serviceId)
	logCtx.WithField("avialableContentIds", avialableContentIds)
	if len(avialableContentIds) == 0 {
		err = fmt.Errorf("No content for campaign %s at all", campaignHash)
		logCtx.WithField("error", err.Error()).Errorf("No content avialabale")
		return err
	}

	retry := 0
findContentId:
	// find first avialable contentId
	contentId := 0
	for id, _ := range avialableContentIds {
		if usedContentIds != nil {
			if _, ok := usedContentIds[id]; ok {
				continue
			}
		}
		contentId = id
		break
	}
	// reset if nothing
	if contentId == 0 {
		logCtx.Debug("No content avialable, reset remembered cache..")
		transactions.Clear(msisdn, serviceId)
		for id, _ := range avialableContentIds {
			contentId = id
			break
		}

	}
	// update in-memory cache usedContentIds
	transactions.Push(msisdn, serviceId, contentId)
	logCtx.WithField("ContentId", contentId).Debug("choosen content")

	path, ok = content.Map[contentId]
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
			return "", err
		}
	}

	s := Subscription{Msisdn: msisdn, ServiceId: serviceId}
	subscriptionId, ok := subscriptions.Map[s.key()]
	if !ok {
		return "", fmt.Errorf("Get subscription: %s", "Not found")
	}

	// record transaction
	ContentSvc.recordContentGiven <- MsgRecordContent{
		Msisdn:         msisdn,
		CampaignId:     campaign.Id,
		ContentId:      contentId,
		ServiceId:      serviceId,
		SubscriptionId: subscriptionId,
	}

	return path, nil
}

// Tasks:
// Keep in memory all active service to content mapping
// Allow to get all content ids of given service id
// Reload when changes to xmp_service_content or xmp_service are done
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
		return fmt.Errorf("xmp_services QueryServices: %s, query: %s", err.Error(), query)
	}
	defer rows.Close()

	var serviceIds []int64
	for rows.Next() {
		var serviceId int64
		if err := rows.Scan(
			&serviceId,
		); err != nil {
			return nil, err
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
		return fmt.Errorf("xmp_service_content QueryServices: %s, query: %s", err.Error(), query)
	}
	defer rows.Close()

	var serviceMap []ServiceContent
	for rows.Next() {
		var serviceContent ServiceContent
		if err := rows.Scan(
			&serviceContent.IdService,
			&serviceContent.IdContent,
		); err != nil {
			return nil, err
		}
		serviceMap = append(serviceMap, serviceContent)
	}
	if rows.Err() != nil {
		return fmt.Errorf("xmp_service_content RowsError: %s", err.Error())
	}

	s.Lock()
	defer s.Unlock()

	s.Map = make(map[string][]Content)
	for _, service := range serviceMap {
		s.Map[service] = append(service.IdContent)
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
// Reload when changes to xmp_content
var content = &Contents{}

type Contents struct {
	sync.RWMutex
	Map map[int64]string
}

func (s Contents) Reload() error {
	query := fmt.Sprintf("select id from %scontent where status = $1", ContentSvc.sConfig.TablePrefix)
	rows, err := ContentSvc.db.Query(query, ACTIVE_STATUS)
	if err != nil {
		return fmt.Errorf("xmp_content QueryServices: %s, query: %s", err.Error(), query)
	}
	defer rows.Close()

	var contents []Content
	for rows.Next() {
		var c Content
		if err := rows.Scan(&c.Id, &c.Object); err != nil {
			return nil, err
		}
		contents = append(contents, c)
	}
	if rows.Err() != nil {
		return fmt.Errorf("RowsError: %s", err.Error())
	}

	s.Lock()
	defer s.Unlock()

	s.Map = make(map[string][]Content)
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
		record := &Campaign{}

		if err := rows.Scan(
			&record.Id,
			&record.Hash,
			&record.ServiceId,
		); err != nil {
			return nil, err
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
		record := &Subscription{}

		if err := rows.Scan(
			&record.Msisdn,
			&record.ServiceId,
			&record.SubscriptionId,
		); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if rows.Err() != nil {
		return fmt.Errorf("Subscriptions Reload RowsError: %s", err.Error())
	}

	s.Lock()
	defer s.Unlock()

	s.Map = make(map[string]Subscription, len(records))
	for _, subscription := range records {
		s.Map[subscription.key()] = subscription.SubscriptionId
	}
	return nil
}

var transactions = &Transactions{}

// When updating from database, reading is forbidden
// Map structure: map [ msisdn + service_id ] []content_id
// where
// * msisdn + service_id -- is a transaction key (see below) (could be changed to msisdn)
// * content_id is content that was shown to msisdn
type Transactions struct {
	sync.RWMutex
	Map map[string][]int64
}

// Transaction Data that neded to build in-memory cache of used content-ids
// and alos need for recording "got content" transaction
type Transaction struct {
	Msisdn         string
	ServiceId      int64
	ContentId      int64
	Type           int
	CountryCode    int
	Status         int
	OperatorCode   int
	SubscriptionId int
}

// Used to get a key of used content ids
// when key == msisdn, then uniq content exactly
// when key == msisdn + service+id, then unique content per sevice
func (t Transaction) key() string {
	return t.Msisdn + "-" + strconv.Atoi(t.ServiceId)
}

// Load transactions to filter content that had been seen by the msisdn.
// transaction type == 1 == download content
// created at == before date specified in config
// status == 1 == successful
func (s Transactions) Reload() error {
	query := fmt.Sprintf("select msisdn, id_service, id_content "+
		"from %stransactions "+
		"where status = $1 and tran_type = $2 and "+
		"created_at > (CURRENT_TIMESTAMP - INTERVAL '$3 days')",
		ContentSvc.sConfig.TablePrefix,
	)
	rows, err := ContentSvc.db.Query(query, 1,
		ContentSvc.sConfig.TransactionType,
		ContentSvc.sConfig.UniqDays)

	if err != nil {
		return fmt.Errorf("Transactions Reload QueryServices: %s, query: %s", err.Error(), query)
	}
	defer rows.Close()

	var records []Transaction
	for rows.Next() {
		record := &Transaction{}

		if err := rows.Scan(
			&record.Msisdn,
			&record.ServiceId,
			&record.ContentId,
		); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if rows.Err() != nil {
		return fmt.Errorf("Transactions Reload RowsError: %s", err.Error())
	}

	s.Lock()
	defer s.Unlock()

	s.Map = make(map[string]map[int64]map[int64]struct{})
	for _, transaction := range records {

		// key == msisdn + service_id, contains content_ids
		if _, ok := s.Map[transaction.key()]; !ok {
			s.Map[transaction.key()] = make(map[string]struct{})
		}
		s.Map[transaction.key()][transaction.ContentId] = struct{}{}
	}
	return nil
}

// Get content ids that was seen by msisdn
// Attention: filtered by service id also,
// so if we would have had content id on one service and the same content id on another service as a content id
// then it had used as different contens! And will shown
func (s Transactions) Get(msisdn string, serviceId int64) (contentIds map[int64]struct{}) {
	var ok bool
	t := Transaction{Msisdn: msisdn, ServiceId: serviceId}
	if contentIds, ok = s.Map[t.key()]; ok {
		return contentIds
	}
	return nil
}

// When there is no content avialabe for the msisdn, reset the content counter
// Breakes after reloading transaction table (on the restart of the application)
func (s Transactions) Clear(msisdn string, serviceId int64) {
	t := Transaction{Msisdn: msisdn, ServiceId: serviceId}
	delete(s.Map, t.key())
}

// After we have chosen the content to show,
// we notice it in transaction table (another place)
// and also we need to update in-memory cache of used content id for this msisdn and service id
func (s Transactions) Push(msisdn string, serviceId int64, contentId int64) {
	t := Transaction{Msisdn: msisdn, ServiceId: serviceId}
	if _, ok := s.Map[t.key()]; !ok {
		s.Map[t.key()] = make(map[int64]struct{})
	}
	s.Map[t.key()][contentId] = struct{}{}
}

func recordContentGiven() {

	go func() {
		for {
			var t MsgRecordContent
			t = <-ContentSvc.recordContentGiven

			query := "INSERT INTO %scontent_sent" +
				" (msisdn, id_campaign, id_service, id_subscription, id_content) values ($1, $2, $3, $4, $5)"

			_, err := ContentSvc.db.Exec(query,
				t.Msisdn, t.CampaignId, t.ServiceId, t.SubscriptionId, t.ContentId)

			if err != nil {
				log.WithFields(log.Fields{"transaction": t, "error": err.Error()}).Error("add transaction")
			}
		}
	}()
}
