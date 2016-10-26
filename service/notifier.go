package service

import (
	"encoding/json"
	"fmt"

	"github.com/vostrok/rabbit"
)

type Notifier interface {
	ContentSentNotify(msg *ContentSentProperties) error
}

type NotifierConfig struct {
	Queues struct {
		ContentSent string `yaml:"content_sent"`
	} `yaml:"queues"`
	Rbmq rabbit.RBMQConfig `yaml:"rabbit"`
}
type queues struct {
	contentSent string
}
type notifier struct {
	q  queues
	mq rabbit.AMQPService
}

type EventNotify struct {
	EventName string      `json:"event_name,omitempty"`
	EventData interface{} `json:"event_data,omitempty"`
}

func NewNotifierService(conf NotifierConfig) Notifier {
	var n Notifier
	{
		rabbit := rabbit.NewPublisher(rabbit.RBMQConfig{
			Url:     conf.Rbmq.Url,
			ChanCap: conf.Rbmq.ChanCap,
			Metrics: rabbit.InitMetrics(),
		})

		n = &notifier{
			q: queues{
				contentSent: conf.Queues.ContentSent,
			},
			mq: rabbit,
		}
	}
	return n
}

func (service notifier) ContentSentNotify(msg *ContentSentProperties) error {

	event := EventNotify{
		EventName: "content_sent",
		EventData: msg,
	}

	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("json.Marshal: %s", err.Error())
	}

	service.mq.Publish(rabbit.AMQPMessage{service.q.contentSent, body})
	return nil
}
