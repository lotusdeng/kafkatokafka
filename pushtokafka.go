package main

import (
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lotusdeng/gocommon"

	"github.com/Shopify/sarama"
	log "github.com/lotusdeng/log4go"
)

func PushMsgFromKafkaLoop(kafkaToKafkaStatus *KafkaToKafkaStatus, quitChannel chan os.Signal) {
	defer gocommon.ExitWaitGroup.Done()

	config := sarama.NewConfig()
	//  config.Producer.RequiredAcks = sarama.WaitForAll
	//  config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true
	config.Producer.Timeout = 5 * time.Second
	switch AppConfigSingleton.Kafka.ApiVersion {
	case "V0_11_0_0":
		config.Version = sarama.V0_11_0_0
	case "V0_11_0_1":
		config.Version = sarama.V0_11_0_1
	case "V1_0_0_0":
		config.Version = sarama.V1_0_0_0
	case "V1_1_0_0":
		config.Version = sarama.V1_1_0_0
	}
	var producer sarama.SyncProducer
	for {
		if gocommon.IsAppQuit() {
			log.Info("producer IsAppQuit is true loop break")
			return
		}
		tmpProducer, err := sarama.NewSyncProducer(strings.Split(kafkaToKafkaStatus.TargetStatus.Address, ","), config)
		if err != nil {
			log.Info("producer connect kafka fail, error:", err)
			if gocommon.IsAppQuit() {
				log.Info("producer IsAppQuit is true loop break")
				return
			}
			time.Sleep(5 * time.Second)
			continue
		} else {
			log.Info("producer connect kafka success")
			producer = tmpProducer
			break
		}
	}
	defer producer.Close()
	for {
		select {
		case receiveMsg := <-kafkaToKafkaStatus.MsgChannel:
			atomic.AddInt64(&kafkaToKafkaStatus.MsgChannelCurrentSize, -1)

			sendMsg := &sarama.ProducerMessage{
				Topic:     kafkaToKafkaStatus.TargetStatus.Topic,
				Value:     sarama.ByteEncoder(receiveMsg.Value),
				Timestamp: time.Now(),
			}

			partitionId, offset, err := producer.SendMessage(sendMsg)
			if err != nil {
				atomic.AddInt64(&kafkaToKafkaStatus.TargetStatus.PutMsgToKafkaFailCount, 1)
				atomic.AddInt64(&AppDataSingleton.PutMsgToKafkaFailCount, 1)
			} else {
				atomic.AddInt64(&kafkaToKafkaStatus.TargetStatus.PutMsgToKafkaSuccessCount, 1)
				atomic.AddInt64(&AppDataSingleton.PutMsgToKafkaSuccessCount, 1)
				log.Info("producer send kafka msg, partition:", partitionId, ", offset:", offset)
				atomic.StoreInt32(&kafkaToKafkaStatus.TargetStatus.LastSendMsgPartition, partitionId)
				atomic.StoreInt64(&kafkaToKafkaStatus.TargetStatus.LastSendMsgOffset, offset)
				atomic.StoreInt64(&AppDataSingleton.LastSendMsgOffset, offset)

			}
		case <-quitChannel:
			log.Info("producer receive quit signal, break loop")
			return

		}

	}

	log.Info("consumer loop exit")
}
