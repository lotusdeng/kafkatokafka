package main

import (
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lotusdeng/gocommon"

	"github.com/Shopify/sarama"
	saramacluter "github.com/bsm/sarama-cluster"
	log "github.com/lotusdeng/log4go"
)

func PullMsgFromKafkaLoop(kafkaToKafkaStatus *KafkaToKafkaStatus, quitChannel chan os.Signal) {
	defer gocommon.ExitWaitGroup.Done()
	config := saramacluter.NewConfig()

	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Consumer.Offsets.CommitInterval = time.Duration(AppConfigSingleton.Kafka.OffsetCommitInterval) * time.Second
	if AppConfigSingleton.Kafka.OffsetInitial == sarama.OffsetNewest {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	} else {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}
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

	brokers := strings.Split(kafkaToKafkaStatus.SourceStatus.Address, ",")
	topics := strings.Split(kafkaToKafkaStatus.SourceStatus.Topic, ",")
	log.Info("consumer start connect kafka, address:", kafkaToKafkaStatus.SourceStatus.Address, ", topic:", kafkaToKafkaStatus.SourceStatus.Topic,
		", group:", kafkaToKafkaStatus.SourceStatus.Group, " offset init:", AppConfigSingleton.Kafka.OffsetInitial, ", commit interval:", AppConfigSingleton.Kafka.OffsetCommitInterval)
	var consumer *saramacluter.Consumer
	for {
		if gocommon.IsAppQuit() {
			log.Info("consumer IsAppQuit is true, loop break")
			return
		}
		tmpConsumer, err := saramacluter.NewConsumer(brokers, kafkaToKafkaStatus.SourceStatus.Group, topics, config)
		if err != nil {
			log.Info("consumer connect kafka fail, error:", err)
			if gocommon.IsAppQuit() {
				log.Info("consumer IsAppQuit is true, loop break")
				return
			}
			time.Sleep(5 * time.Second)
			continue
		} else {
			log.Info("consumer connect kafka success")
			consumer = tmpConsumer
			break
		}
	}
	defer consumer.Close()

	// consume errors
	go func() {
		for err := range consumer.Errors() {
			log.Errorf("consumer receive Errors:%s", err.Error())
		}
		log.Info("consumer error loop exit")
	}()

	// consume notifications
	go func() {
		for ntf := range consumer.Notifications() {
			log.Infof("consumer receive Notifications: %+v", ntf)
			if ntf.Type == saramacluter.RebalanceOK {
				for topicName, partitionMap := range ntf.Current {
					if kafkaToKafkaStatus.SourceStatus.Topic == topicName {
						for key, _ := range kafkaToKafkaStatus.SourceStatus.BalancePartitionIdMap {
							delete(kafkaToKafkaStatus.SourceStatus.BalancePartitionIdMap, key)
						}
						for _, partitionId := range partitionMap {
							kafkaToKafkaStatus.SourceStatus.BalancePartitionIdMap[partitionId] = 1
						}
					}

				}

			}
		}
		log.Info("consumer notification loop exit")
	}()

	// consume messages, watch signals
	for {
		if gocommon.IsAppQuit() {
			return
		}
		if atomic.LoadInt64(&AppDataSingleton.PauseGetMsgFromKafka) == 1 {
			time.Sleep(1 * time.Second)
			continue
		}
		select {
		case msg, ok := <-consumer.Messages():
			if !ok {
				log.Error("consumer break loop")
				return
			}
			atomic.AddInt64(&AppDataSingleton.GetMsgFromtKafkaCount, 1)
			atomic.AddInt64(&kafkaToKafkaStatus.SourceStatus.GetMsgFromtKafkaCount, 1)
			log.Info("consumer msg, partition:", msg.Partition, ", offset:", msg.Offset, ", time:", msg.Timestamp)
			{
				kafkaToKafkaStatus.SourceStatus.PartitionStatusMapMutex.Lock()
				item, exist := kafkaToKafkaStatus.SourceStatus.PartitionStatusMap[msg.Partition]
				if exist {
					item.CurrentOffset = msg.Offset
					item.Timestamp = msg.Timestamp
				} else {
					kafkaToKafkaStatus.SourceStatus.PartitionStatusMap[msg.Partition] = &PartitionStatus{
						Id:            msg.Partition,
						CurrentOffset: msg.Offset,
						Timestamp:     msg.Timestamp,
					}
					atomic.AddInt64(&kafkaToKafkaStatus.SourceStatus.PartitionCount, 1)
				}
				kafkaToKafkaStatus.SourceStatus.PartitionStatusMapMutex.Unlock()
			}
			atomic.StoreInt64(&kafkaToKafkaStatus.SourceStatus.LastKafkaMsgPartition, int64(msg.Partition))
			atomic.StoreInt64(&kafkaToKafkaStatus.SourceStatus.LastKafkaMsgOffset, msg.Offset)
			if msg.Timestamp.Unix() > 0 {
				atomic.StoreInt64(&kafkaToKafkaStatus.SourceStatus.LastKafkaMsgTimestampBySecond, msg.Timestamp.Unix())
			}

			kafkaToKafkaStatus.MsgChannel <- msg
			atomic.AddInt64(&kafkaToKafkaStatus.MsgChannelCurrentSize, 1)
			consumer.MarkOffset(msg, "") // mark message as processed
		case <-quitChannel:
			log.Info("consumer receive quit signal, break loop")
			return
		}
	}
	log.Info("consumer loop exit")
}
