package main

import (
	"encoding/xml"
	"os"

	"github.com/lotusdeng/gocommon"

	"github.com/Shopify/sarama"
	log "github.com/lotusdeng/log4go"
)

func main() {
	log.LoadConfiguration("config/kafkatokafka.xml", "xml")
	log.SetGlobalLevel(log.INFO)
	defer log.Close()

	log.Info("main start, version:", AppVersion)

	log.Info("main start open config file")
	xmlFile, err := os.Open("config/kafkatokafka.xml")
	if err != nil {
		log.Error("main load config file fail, error:", err.Error())
		return
	}
	log.Info("main open config success")
	defer xmlFile.Close()

	if err := xml.NewDecoder(xmlFile).Decode(&AppConfigSingleton); err != nil {
		log.Error("main decode config file fail, error:", err.Error())
		return
	}
	log.Info("main decode config from file success")
	log.Info("main config.kafka apiversion:", AppConfigSingleton.Kafka.ApiVersion)

	gocommon.InitAppQuit()
	defer gocommon.UinitAppQuit()

	log.Info("main http url:http://127.0.0.1:", AppConfigSingleton.HttpPort)
	gocommon.ExitWaitGroup.Add(1)
	go HttpServerLoop(AppConfigSingleton.HttpPort, gocommon.GlobalQuitChannel)

	for _, kafkaToKafka := range AppConfigSingleton.KafkaToKafkas {
		kafkaToKafkaStatus := KafkaToKafkaStatus{}
		kafkaToKafkaStatus.SourceStatus.Address = kafkaToKafka.Source.Address
		kafkaToKafkaStatus.SourceStatus.Topic = kafkaToKafka.Source.Topic
		kafkaToKafkaStatus.SourceStatus.Group = kafkaToKafka.Source.Group
		kafkaToKafkaStatus.SourceStatus.PartitionStatusMap = make(map[int32]*PartitionStatus)
		kafkaToKafkaStatus.SourceStatus.BalancePartitionIdMap = make(map[int32]int)
		kafkaToKafkaStatus.TargetStatus.Address = kafkaToKafka.Target.Address
		kafkaToKafkaStatus.TargetStatus.Topic = kafkaToKafka.Target.Topic
		kafkaToKafkaStatus.MsgChannel = make(chan *sarama.ConsumerMessage, AppConfigSingleton.MsgItemChannelMaxSize)
		AppDataSingleton.KafkaToKafkaStatusList = append(AppDataSingleton.KafkaToKafkaStatusList, &kafkaToKafkaStatus)

		for i := 0; i < kafkaToKafka.Target.ClientCount; i += 1 {
			gocommon.ExitWaitGroup.Add(1)
			go PushMsgFromKafkaLoop(&kafkaToKafkaStatus, gocommon.GlobalQuitChannel)
		}

		for i := 0; i < kafkaToKafka.Source.ClientCount; i += 1 {
			gocommon.ExitWaitGroup.Add(1)
			go PullMsgFromKafkaLoop(&kafkaToKafkaStatus, gocommon.GlobalQuitChannel)
		}
	}

	if AppConfigSingleton.AutoRestart.Enable {
		log.Info("main start auto restart")
		gocommon.ExitWaitGroup.Add(1)
		go AutoRestartLoop(gocommon.GlobalQuitChannel)
	}
	gocommon.ExitWaitGroup.Wait()
	log.Info("main exit")
}
