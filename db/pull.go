package db

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
)

const ACTIVE_STATUS = 1

type CQRRequest struct {
	Table string `json:"table,omitempty"`
}

func InitCQR() error {
	if err := campaign.Reload(); err != nil {
		return fmt.Errorf("campaignsMap.Reload: %s", err.Error())
	}
	return nil
}

// Get Content by campaign hash
// Also, record transaction: content was shown to the user
//
func GetContentByCampaignHash(msisdn, campaignHash string) (path string, err error) {
	// serviceId, ok := campaign.Map[campaignHash]
	//if !ok {
	//	return "", fmt.Errorf("CampaignHash: %s", "Not found")
	//}
	// usedContentIds := transactions.Get(msisdn, serviceId)

	// avialableContentIds := service.Get(serviceId)

	// find avialableContentIds minus usedContentIds
	// reset if nothing
	// record transaction
	// update in-memory cache usedContentIds

	return path, errors.New("Not Implemented")
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
	query := "select id from xmp_services where status = $1"
	rows, err := d.db.Query(query, ACTIVE_STATUS)
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

	query = "select id_service, id_content from xmp_service_content where status = $1 and id_service IN ()"
	rows, err = d.db.Query(query, ACTIVE_STATUS, serviceIds)
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
	query := "select id from xmp_content where status = $1"
	rows, err := d.db.Query(query, ACTIVE_STATUS)
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
	Map map[string]int64
}
type Campaign struct {
	Hash      string
	ServiceId int64
}

func (s Campaigns) Reload() error {
	query := "select hash, service_id_1 from xmp_campaigns where status = $1"
	rows, err := d.db.Query(query, ACTIVE_STATUS)
	if err != nil {
		return fmt.Errorf("QueryServices: %s, query: %s", err.Error(), query)
	}
	defer rows.Close()

	var records []Campaign
	for rows.Next() {
		record := &Campaign{}

		if err := rows.Scan(
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
		s.Map[campaign.Hash] = campaign.ServiceId
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
type Transaction struct {
	Msisdn    string
	ServiceId int64
	ContentId int64
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
	query := "select msisdn, id_service, id_content " +
		"from xmp_transactions " +
		"where status = $1 and tran_type = $2 and created_at > $3"

	rows, err := d.db.Query(query, 1, 1)
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

	s.Map = make(map[string]map[int64]*[]int64)
	for _, transaction := range records {

		if _, ok := s.Map[transaction.key()]; !ok {
			s.Map[transaction.key()] = []int64{}
		}
		s.Map[transaction.key()] = append(s.Map[transaction.key()], transaction.ContentId)
	}
	return nil
}

// Get content ids that was seen by msisdn
// Attention: filtered by service id also,
// so if we would have had content id on one service and the same content id on another service as a content id
// then it had used as different contens! And will shown
func (s Transactions) Get(msisdn string, serviceId int64) (contentIds []int64) {
	var ok bool
	t := Transaction{Msisdn: msisdn, ServiceId: serviceId}
	if contentIds, ok = s.Map[t.key()]; ok {
		return contentIds
	}
	return contentIds
}

// When there is no content avialabe for the msisdn, reset the content counter
// Breakes after reloading transaction table (on the restart of the application)
func (s Transactions) Clear(msisdn string, serviceId int64) {
	t := Transaction{Msisdn: msisdn, ServiceId: serviceId}
	delete(s.Map, t.key())
}

// After we have choosen the content to show,
// we notice it in transaction table (another place)
// and also we need to update in-memory cache of used content id for this msisdn and service id
func (s Transactions) Push(msisdn string, serviceId int64, contentId int64) {
	t := Transaction{Msisdn: msisdn, ServiceId: serviceId}
	if _, ok := s.Map[t.key()]; !ok {
		s.Map[t.key()] = []int64{}
	}
	s.Map[t.key()] = append(s.Map[t.key()], contentId)
}
