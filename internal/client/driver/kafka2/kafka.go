package kafka2

import (
	"github.com/Shopify/sarama"
)

type SchemaType string

const (
	CONVERTER_JSON = "json"
	CONVERTER_AVRO = "avro"

	SCHEMA_TYPE_STRUCT = "struct"
)

type KafkaConfig struct {
	Broker string
	Topic  string
	Converter string
}

type KafkaManager struct {
	cfg      *KafkaConfig
	producer sarama.SyncProducer
}

func NewKafkaManager(kcfg *KafkaConfig) (*KafkaManager, error) {
	var err error
	k := &KafkaManager{
		cfg: kcfg,
	}
	config := sarama.NewConfig()

	k.producer, err = sarama.NewSyncProducer([]string{kcfg.Broker}, config)
	if err != nil {
		return nil, err
	}
	return k, nil
}

func (k *KafkaManager) Send(key []byte, value []byte) error {
	msg := &sarama.ProducerMessage{
		Topic:     k.cfg.Topic,
		Partition: int32(-1),
		Key:       sarama.ByteEncoder(key),
		Value:     sarama.ByteEncoder(value),
	}

	_, _, err := k.producer.SendMessage(msg)
	if err != nil {
		return err
	}

	// TODO partition? offset?
	return nil
}

type Schema struct {
	Type string
	Fields []*Schema `json:"fields,omitempty"`
	Optional
}
