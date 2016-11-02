package service

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"

	"database/sql"
	"github.com/vostrok/contentd/server/src/metrics"
)

func init() {
	log.SetLevel(log.DebugLevel)
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
	switch {
	case strings.Contains(table, "campaign"):
		if err := campaign.Reload(); err != nil {
			return false, fmt.Errorf("campaign.Reload: %s", err.Error())
		}
	case strings.Contains(table, "service"):
		if err := service.Reload(); err != nil {
			return false, fmt.Errorf("service.Reload: %s", err.Error())
		}
	case strings.Contains(table, "content"):
		if err := content.Reload(); err != nil {
			return false, fmt.Errorf("content.Reload: %s", err.Error())
		}
	case strings.Contains(table, "service_content"):
		if err := service.Reload(); err != nil {
			return false, fmt.Errorf("service_content: service.Reload: %s", err.Error())
		}
	case strings.Contains(table, "content_sent"):
		if err := contentSent.Reload(); err != nil {
			return false, fmt.Errorf("sentContent.Reload: %s", err.Error())
		}
	default:
		return false, fmt.Errorf("CQR Request: Unknown table: %s", table)
	}

	return true, nil
}

func AddCQRHandler(r *gin.Engine) {
	rg := r.Group("/cqr")
	rg.GET("", Reload)
}

type response struct {
	Success bool        `json:"success,omitempty"`
	Err     error       `json:"error,omitempty"`
	Data    interface{} `json:"data,omitempty"`
	Status  int         `json:"-"`
}

func Reload(c *gin.Context) {
	var err error
	r := response{Err: err, Status: http.StatusOK}

	table, exists := c.GetQuery("table")
	if !exists || table == "" {
		table, exists = c.GetQuery("t")
		if !exists || table == "" {
			err := errors.New("Table name required")
			r.Status = http.StatusBadRequest
			r.Err = err
			render(r, c)
			return
		}
	}

	begin := time.Now()
	defer func() {
		metrics.M.CQRRequest.Time.CatchOverTime(time.Since(begin), time.Second*time.Duration(10))
	}()
	metrics.M.CQRRequest.Count.Add(1)

	success, err := CQR(table)
	if err != nil {
		metrics.M.CQRRequest.Errors.Add(1)
		err := fmt.Errorf("CQR table %s: %s", table, err.Error())
		r.Status = http.StatusInternalServerError
		r.Err = err
		render(r, c)
	}
	r.Success = success
	render(r, c)
	return
}

func render(msg response, c *gin.Context) {
	if msg.Err != nil {
		c.Header("Error", msg.Err.Error())
		c.Error(msg.Err)
	}
	c.JSON(msg.Status, msg)
}

// Tasks:
// Keep in memory all active service to content mapping
// Allow to get all content ids of given service id
// Reload when changes to service_content or service are done
var service = &Services{}

type Services struct {
	sync.RWMutex
	Map map[int64]Service
}
type Service struct {
	Id         int64
	Price      float64
	PaidHours  int
	DelayHours int
	ContentIds []int64
}
type Content struct {
	Id   int64
	Path string
	Name string
}
type ServiceContent struct {
	IdService int64
	IdContent int64
}

func (s *Services) Reload() (err error) {
	log.WithFields(log.Fields{}).Debug("services reload...")
	begin := time.Now()
	defer func(err error) {
		errStr := ""
		if err != nil {
			errStr = err.Error()
		}
		log.WithFields(log.Fields{
			"error": errStr,
			"took":  time.Since(begin),
		}).Debug("service reload")
	}(err)

	query := fmt.Sprintf("SELECT "+
		"id, "+
		"paid_hours, "+
		"delay_hours, "+
		"price "+
		"from %sservices where status = $1",
		ContentSvc.dbConf.TablePrefix)
	var rows *sql.Rows
	rows, err = ContentSvc.db.Query(query, ACTIVE_STATUS)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}
	defer rows.Close()

	var svcs []Service
	for rows.Next() {
		var srv Service
		if err = rows.Scan(
			&srv.Id,
			&srv.PaidHours,
			&srv.DelayHours,
			&srv.Price,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return err
		}
		svcs = append(svcs, srv)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}
	svcMap := make(map[int64]Service)
	for _, v := range svcs {
		svcMap[v.Id] = v
	}

	serviceIdsStr := []string{}
	for _, v := range svcs {
		serviceIdsStr = append(serviceIdsStr, strconv.FormatInt(v.Id, 10))
	}
	query = fmt.Sprintf("select id_service, id_content from %sservice_content where status = $1"+
		" and id_service = any($2::integer[])", ContentSvc.dbConf.TablePrefix)
	rows, err = ContentSvc.db.Query(query, ACTIVE_STATUS, "{"+strings.Join(serviceIdsStr, ", ")+"}")
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}
	defer rows.Close()

	var serviceContentAr []ServiceContent
	for rows.Next() {
		var serviceContent ServiceContent
		if err = rows.Scan(
			&serviceContent.IdService,
			&serviceContent.IdContent,
		); err != nil {
			err = fmt.Errorf("rows.Scan %s", err.Error())
			return
		}
		serviceContentAr = append(serviceContentAr, serviceContent)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Error: %s", err.Error())
		return
	}

	s.Lock()
	defer s.Unlock()

	s.Map = make(map[int64]Service)
	for _, serviceContent := range serviceContentAr {
		srv, ok := s.Map[serviceContent.IdService]
		if !ok {
			s.Map[serviceContent.IdService] = Service{}
		}
		srv.ContentIds = append(srv.ContentIds, serviceContent.IdContent)
		srv.Id = serviceContent.IdService
		svc := svcMap[serviceContent.IdService]
		srv.Price = svc.Price
		srv.DelayHours = svc.DelayHours
		srv.PaidHours = svc.PaidHours
		s.Map[serviceContent.IdService] = srv

	}
	return nil
}

func (s *Services) Get(serviceId int64) (contentIds []int64) {
	if svc, ok := s.Map[serviceId]; ok {
		return svc.ContentIds
	}
	return []int64{}
}

// Tasks:
// Keep in memory all active content_ids mapping to their object string (url path to content)
// Allow to get object for given content id
// Reload when changes to content
var content = &Contents{}

type Contents struct {
	sync.RWMutex
	Map map[int64]Content
}

func (s *Contents) Reload() (err error) {
	log.WithFields(log.Fields{}).Debug("content reload...")
	begin := time.Now()
	defer func(err error) {
		errStr := ""
		if err != nil {
			errStr = err.Error()
		}
		log.WithFields(log.Fields{
			"error": errStr,
			"took":  time.Since(begin),
		}).Debug("content reload")
	}(err)

	query := fmt.Sprintf("select "+
		"id, "+
		"object, "+
		"content_name "+
		"from %scontent where status = $1",
		ContentSvc.dbConf.TablePrefix)

	var rows *sql.Rows
	rows, err = ContentSvc.db.Query(query, ACTIVE_STATUS)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}
	defer rows.Close()

	var contents []Content
	for rows.Next() {
		var c Content
		if err = rows.Scan(
			&c.Id,
			&c.Path,
			&c.Name,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return
		}
		contents = append(contents, c)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}

	s.Lock()
	defer s.Unlock()

	s.Map = make(map[int64]Content)
	for _, content := range contents {
		s.Map[content.Id] = content
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

func (s *Campaigns) Reload() (err error) {
	log.WithFields(log.Fields{}).Debug("campaign reload...")
	begin := time.Now()
	defer func(err error) {
		errStr := ""
		if err != nil {
			errStr = err.Error()
		}
		log.WithFields(log.Fields{
			"error": errStr,
			"took":  time.Since(begin),
		}).Debug("campaign reload")
	}(err)

	query := fmt.Sprintf("select id, hash, service_id_1 from %scampaigns where status = $1",
		ContentSvc.dbConf.TablePrefix)
	var rows *sql.Rows
	rows, err = ContentSvc.db.Query(query, ACTIVE_STATUS)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}
	defer rows.Close()

	var records []Campaign
	for rows.Next() {
		record := Campaign{}
		if err = rows.Scan(
			&record.Id,
			&record.Hash,
			&record.ServiceId,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return
		}
		records = append(records, record)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}

	s.Lock()
	defer s.Unlock()

	s.Map = make(map[string]Campaign, len(records))
	for _, campaign := range records {
		s.Map[campaign.Hash] = campaign
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
	Price          int    `json:"price"`
	ContentPath    string `json:"content_path"`
	ContentName    string `json:"content_name"`
	CapmaignHash   string `json:"capmaign_hash"`
	CampaignId     int64  `json:"campaign_id"`
	ContentId      int64  `json:"content_id"`
	ServiceId      int64  `json:"service_id"`
	SubscriptionId int64  `json:"subscription_id"`
	CountryCode    int64  `json:"country_code"`
	OperatorCode   int64  `json:"operator_code"`
	PaidHours      int    `json:"paid_hours"`
	DelayHours     int    `json:"delay_hours"`
}

// Used to get a key of used content ids
// when key == msisdn, then uniq content exactly
// when key == msisdn + service+id, then unique content per sevice
func (t ContentSentProperties) key() string {
	return t.Msisdn + "-" + strconv.FormatInt(t.ServiceId, 10)
}

// Load sent contents to filter content that had been seen by the msisdn.
// created at == before date specified in config
func (s *SentContents) Reload() (err error) {
	log.WithFields(log.Fields{}).Debug("content_sent reload...")
	begin := time.Now()
	defer func(err error) {
		errStr := ""
		if err != nil {
			errStr = err.Error()
		}
		log.WithFields(log.Fields{
			"error": errStr,
			"took":  time.Since(begin),
		}).Debug("content_sent reload")
	}(err)

	query := fmt.Sprintf("select msisdn, id_service, id_content "+
		"from %scontent_sent "+
		"where sent_at > (CURRENT_TIMESTAMP - INTERVAL '"+
		strconv.Itoa(ContentSvc.sConfig.UniqDays)+" days')",
		ContentSvc.dbConf.TablePrefix)

	var rows *sql.Rows
	rows, err = ContentSvc.db.Query(query)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}
	defer rows.Close()

	var records []ContentSentProperties
	for rows.Next() {
		record := ContentSentProperties{}

		if err = rows.Scan(
			&record.Msisdn,
			&record.ServiceId,
			&record.ContentId,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return
		}
		records = append(records, record)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
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
func (s *SentContents) Get(msisdn string, serviceId int64) (contentIds map[int64]struct{}) {
	var ok bool
	t := ContentSentProperties{Msisdn: msisdn, ServiceId: serviceId}
	log.WithFields(log.Fields{
		"key": t.key(),
	}).Debug("get contents")
	if contentIds, ok = s.Map[t.key()]; ok {
		return contentIds
	}
	return nil
}

// When there is no content avialabe for the msisdn, reset the content counter
// Breakes after reloading sent content table (on the restart of the application)
func (s *SentContents) Clear(msisdn string, serviceId int64) {
	t := ContentSentProperties{Msisdn: msisdn, ServiceId: serviceId}
	log.WithFields(log.Fields{
		"key": t.key(),
	}).Debug("reset cache")
	delete(s.Map, t.key())
}

//WithFields(log.Fields{})

// After we have chosen the content to show,
// we notice it in sent content table (another place)
// and also we need to update in-memory cache of used content id for this msisdn and service id
func (s *SentContents) Push(msisdn string, serviceId int64, contentId int64) {
	t := ContentSentProperties{Msisdn: msisdn, ServiceId: serviceId}
	log.WithFields(log.Fields{
		"key": t.key(),
	}).Debug("push contentid")
	if _, ok := s.Map[t.key()]; !ok {
		s.Map[t.key()] = make(map[int64]struct{})
	}
	s.Map[t.key()][contentId] = struct{}{}
}
