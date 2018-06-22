package main

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

type PartitionStatus struct {
	Id            int32
	CurrentOffset int64
	HighOffset    int64
	LowOffset     int64
	Timestamp     time.Time
}

type SourceStatus struct {
	Address                       string
	Topic                         string
	Group                         string
	TotalCurrentOffset            int64
	LastViewTotalCurrentOffset    int64
	LastViewTime                  time.Time
	TotalHighOffset               int64
	PartitionCount                int64
	PartitionStatusMap            map[int32]*PartitionStatus
	PartitionStatusMapMutex       sync.Mutex
	LastKafkaMsgPartition         int64
	LastKafkaMsgOffset            int64
	LastKafkaMsgTimestampBySecond int64

	BalancePartitionIdMap map[int32]int
	GetMsgFromtKafkaCount int64
	PutMsgToKafkaCount    int64
}

type TargetStatus struct {
	Address                   string
	Topic                     string
	PutMsgToKafkaSuccessCount int64
	PutMsgToKafkaFailCount    int64
	LastSendMsgPartition      int32
	LastSendMsgOffset         int64
}

type KafkaToKafkaStatus struct {
	SourceStatus          SourceStatus
	TargetStatus          TargetStatus
	MsgChannel            chan *sarama.ConsumerMessage
	MsgChannelMaxSize     int64
	MsgChannelCurrentSize int64
}

type AppData struct {
	AppStartTime                      time.Time
	KafkaToKafkaStatusList            []*KafkaToKafkaStatus
	PauseGetMsgFromKafka              int64
	GetMsgFromtKafkaCount             int64
	PutMsgToKafkaSuccessCount         int64
	LastSendMsgOffset                 int64
	LastViewPutMsgToKafkaSuccessCount int64
	LastViewTime                      time.Time
	PutMsgToKafkaFailCount            int64
}

var AppDataSingleton = AppData{AppStartTime: time.Now()}
