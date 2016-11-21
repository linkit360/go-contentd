package service

import (
	"encoding/json"
	"fmt"

	"github.com/vostrok/utils/amqp"
)

type Notifier interface {
	ContentSentNotify(msg ContentSentProperties) error
}

type NotifierConfig struct {
	Queues struct {
		ContentSent string `yaml:"content_sent"`
	} `yaml:"queues"`
	Rbmq amqp.NotifierConfig `yaml:"rbmq"`
}
type queues struct {
	contentSent string
}
type notifier struct {
	q  queues
	mq *amqp.Notifier
}

type EventNotify struct {
	EventName string      `json:"event_name,omitempty"`
	EventData interface{} `json:"event_data,omitempty"`
}

func NewNotifierService(conf NotifierConfig) Notifier {
	var n Notifier
	{
		rabbit := amqp.NewNotifier(conf.Rbmq)
		n = &notifier{
			q: queues{
				contentSent: conf.Queues.ContentSent,
			},
			mq: rabbit,
		}
	}
	return n
}

func (service notifier) ContentSentNotify(msg ContentSentProperties) error {

	event := EventNotify{
		EventName: "content_sent",
		EventData: msg,
	}

	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("json.Marshal: %s", err.Error())
	}

	service.mq.Publish(amqp.AMQPMessage{service.q.contentSent, body})
	return nil
}
