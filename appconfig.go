package main

type KafkaConfig struct {
	OffsetInitial        int64  `xml:"offsetInitial"`
	OffsetCommitInterval int    `xml:"offsetCommitInterval"`
	ClientCount          int    `xml:"clientCount"`
	ApiVersion           string `xml:"apiVersion"`
}

type AutoRestartConfig struct {
	Enable         bool  `xml:"enable"`
	ByDurationHour int64 `xml:"byDurationHour"`
	ByMsgCount     int64 `xml:"byMsgCount"`
}

type SourceConfig struct {
	Address     string `xml:"address"`
	Topic       string `xml:"topic"`
	Group       string `xml:"group"`
	ClientCount int    `xml:"clientCount"`
}

type TargetConfig struct {
	Address     string `xml:"address"`
	Topic       string `xml:"topic"`
	ClientCount int    `xml:"clientCount"`
}

type KafkaToKafkaConfig struct {
	Source SourceConfig `xml:"source"`
	Target TargetConfig `xml:"target"`
}

type AppConfig struct {
	HttpPort                  string               `xml:"httoPort"`
	Kafka                     KafkaConfig          `xml:"kafka"`
	KafkaToKafkas             []KafkaToKafkaConfig `xml:"kafkaToKafkas>kafkaToKafka"`
	MsgItemChannelMaxSize     uint32               `xml:"msgItemChannelMaxSize"`
	ConsumeMsgTimeoutBySecond int                  `xml:"consumeMsgTimeoutBySecond"`
	AutoRestart               AutoRestartConfig    `xml:"autoRestart"`
}

var AppConfigSingleton AppConfig
