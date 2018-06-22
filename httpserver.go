package main

import (
	"fmt"
	"html/template"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/lotusdeng/gocommon"

	log "github.com/lotusdeng/log4go"
)

type AppInfoItem struct {
	Name  string
	Value string
}

type AppStatusItem struct {
	Name  string
	Value string
}

type AppUrlItem struct {
	Name string
	Url  string
}

type AppConfItem struct {
	ConfName  string
	ConfKey   string
	ConfValue string
}

type PartitionStatusItem struct {
	Id            int32
	CurrentOffset int64
	HighOffset    int64
}
type PartitionStatusItemSlice []PartitionStatusItem

func (a PartitionStatusItemSlice) Len() int { // 重写 Len() 方法
	return len(a)
}
func (a PartitionStatusItemSlice) Swap(i, j int) { // 重写 Swap() 方法
	a[i], a[j] = a[j], a[i]
}
func (a PartitionStatusItemSlice) Less(i, j int) bool { // 重写 Less() 方法， 从大到小排序
	return a[j].Id > a[i].Id
}

type KafkaToKafkaStatusItem struct {
	SourceAddress                   string
	SourceTopic                     string
	SourceGroup                     string
	SourcePartitionCount            int64
	SourceTotalCurrentOffset        int64
	SourceTotalHighOffset           int64
	SourceGetMsgFromKafkaCount      int64
	SourceLastMsgTimestamp          string
	SourcePartitionStatusItemList   []PartitionStatusItem
	MsgChannelCurrentSize           int64
	TargetAddress                   string
	TargetTopic                     string
	TargetPutMsgToKafkaSuccessCount int64
	TargetPutMsgToKafkaFailCount    int64
}

func confHandle(w http.ResponseWriter, req *http.Request) {
	if req.Method == "GET" {
		fileBody, err := ioutil.ReadFile("html/conf.html")
		if err != nil {
			w.Write([]byte("read html/conf.html fail"))
			return
		}
		t, err := template.New("webpage").Parse(string(fileBody[:]))

		data := struct {
			Title           string
			AppConfItemList []AppConfItem
		}{
			Title: "KafkaProxy",
			AppConfItemList: []AppConfItem{
				{
					"日志级别[TRACE|DEBUG|INFO|ERROR]",
					"LogLevel",
					fmt.Sprint(log.GetGlobalLevel()),
				},
				{
					"暂停从kafka获取消息[1|0]",
					"PauseGetMsgFromKafka",
					strconv.FormatInt(atomic.LoadInt64(&AppDataSingleton.PauseGetMsgFromKafka), 10),
				},
			},
		}

		err = t.Execute(w, data)
	} else {
		var (
			confKey   string = req.PostFormValue("ConfKey")
			confValue string = req.PostFormValue(confKey)
		)
		log.Info("http confKey:", confKey, ", confValue:", confValue)
		switch confKey {
		case "LogLevel":
			switch confValue {
			case "TRACE":
				log.SetGlobalLevel(log.TRACE)
			case "DEBUG":
				log.SetGlobalLevel(log.DEBUG)
			case "INFO":
				log.SetGlobalLevel(log.INFO)
			case "ERROR":
				log.SetGlobalLevel(log.ERROR)
			}
		case "PauseGetMsgFromKafka":
			switch confValue {
			case "1":
				atomic.StoreInt64(&AppDataSingleton.PauseGetMsgFromKafka, 1)
			case "0":
				atomic.StoreInt64(&AppDataSingleton.PauseGetMsgFromKafka, 0)
			}

		}

		w.Write([]byte("修改成功"))
	}

}

func getTotalSendSpeed() int64 {
	var speedPersecond int64
	if atomic.LoadInt64(&AppDataSingleton.LastViewPutMsgToKafkaSuccessCount) == 0 {

	} else {
		countDuration := atomic.LoadInt64(&AppDataSingleton.PutMsgToKafkaSuccessCount) - atomic.LoadInt64(&AppDataSingleton.LastViewPutMsgToKafkaSuccessCount)
		timeDuration := time.Now().Sub(AppDataSingleton.LastViewTime).Seconds()
		speedPersecond = int64(float64(countDuration) / timeDuration)
	}
	AppDataSingleton.LastViewTime = time.Now()
	atomic.StoreInt64(&AppDataSingleton.LastViewPutMsgToKafkaSuccessCount, atomic.LoadInt64(&AppDataSingleton.PutMsgToKafkaSuccessCount))
	return speedPersecond
}

func indexHandle(w http.ResponseWriter, req *http.Request) {
	log.Info("http /")
	fileBody, err := ioutil.ReadFile("html/kafkatokafka_index.html")
	if err != nil {
		w.Write([]byte("dpssgate read html/kafkatokafka_index.html fail"))
		return
	}
	t, err := template.New("webpage").Parse(string(fileBody[:]))

	data := struct {
		AppInfoItemList            []AppInfoItem
		AppConfItemList            []AppConfItem
		AppStatusItemList          []AppStatusItem
		AppUrlItemList             []AppUrlItem
		KafkaToKafkaStatusItemList []KafkaToKafkaStatusItem
	}{
		AppInfoItemList: []AppInfoItem{
			{
				"版本",
				AppVersion,
			},
			{
				"程序启动时间",
				AppDataSingleton.AppStartTime.Format("2006-01-02T15:04:05"),
			},
			{
				"当前时间",
				time.Now().Format("2006-01-02T15:04:05"),
			},
			{
				"运行时间",
				func() string {
					du := time.Now().Sub(AppDataSingleton.AppStartTime)
					return fmt.Sprintf("%d天%d小时%d分%d秒", int64(du.Hours())/24, int64(du.Hours())%24, int64(du.Minutes())%60, int64(du.Seconds())%60)
				}(),
			},
		},
		AppConfItemList: []AppConfItem{

			{
				"消息队列最大长度",
				"",
				fmt.Sprint(AppConfigSingleton.MsgItemChannelMaxSize),
			},
		},
		AppStatusItemList: []AppStatusItem{
			{
				"收到的Kafka消息个数",
				strconv.FormatInt(atomic.LoadInt64(&AppDataSingleton.GetMsgFromtKafkaCount), 10),
			},
			{
				"发送成功的Kafka消息个数",
				strconv.FormatInt(atomic.LoadInt64(&AppDataSingleton.PutMsgToKafkaSuccessCount), 10),
			},
			{
				"发送失败的Kafka消息个数",
				strconv.FormatInt(atomic.LoadInt64(&AppDataSingleton.PutMsgToKafkaFailCount), 10),
			},
			{
				"发送Kafka消息Offset",
				strconv.FormatInt(atomic.LoadInt64(&AppDataSingleton.LastSendMsgOffset), 10),
			},
			{
				"发送Kafka消息速度",
				fmt.Sprint(getTotalSendSpeed()),
			},
		},
		AppUrlItemList: []AppUrlItem{
			{
				"强制停止服务",
				"/stop",
			},
			{
				"优雅停止服务",
				"/safestop",
			},
			{
				"修改配置",
				"/conf",
			},
		},
	}
	for _, kafkaToKafkaStatus := range AppDataSingleton.KafkaToKafkaStatusList {
		data.KafkaToKafkaStatusItemList = append(data.KafkaToKafkaStatusItemList, KafkaToKafkaStatusItem{
			SourceAddress:              kafkaToKafkaStatus.SourceStatus.Address,
			SourceTopic:                kafkaToKafkaStatus.SourceStatus.Topic,
			SourceGroup:                kafkaToKafkaStatus.SourceStatus.Group,
			SourceGetMsgFromKafkaCount: atomic.LoadInt64(&kafkaToKafkaStatus.SourceStatus.GetMsgFromtKafkaCount),

			TargetAddress:                   kafkaToKafkaStatus.TargetStatus.Address,
			TargetTopic:                     kafkaToKafkaStatus.TargetStatus.Topic,
			TargetPutMsgToKafkaFailCount:    atomic.LoadInt64(&kafkaToKafkaStatus.TargetStatus.PutMsgToKafkaFailCount),
			TargetPutMsgToKafkaSuccessCount: atomic.LoadInt64(&kafkaToKafkaStatus.TargetStatus.PutMsgToKafkaSuccessCount),
		})
	}

	err = t.Execute(w, data)

}

func stopHandle(w http.ResponseWriter, req *http.Request) {
	log.Warn("http /stop")
	w.Write([]byte("stop ok"))
	gocommon.SignalAppQuit()
	os.Exit(1)
}

func safeStopHandle(w http.ResponseWriter, req *http.Request) {
	log.Warn("http /stop")
	w.Write([]byte("stop ok"))
	SafeStop()
}

func produceHandle(w http.ResponseWriter, req *http.Request) {

}

func HttpServerLoop(httpPort string, quitChannel chan os.Signal) {
	defer gocommon.ExitWaitGroup.Done()
	http.HandleFunc("/", indexHandle)
	http.HandleFunc("/stop", stopHandle)
	http.HandleFunc("/safestop", safeStopHandle)
	http.HandleFunc("/conf", confHandle)
	//var dir = path.Dir(os.Args[0])
	//log.Info(dir)
	//http.Handle("/file/", http.FileServer(http.Dir(dir)))

	httpServer := &http.Server{
		Addr:    ":" + AppConfigSingleton.HttpPort,
		Handler: http.DefaultServeMux,
	}

	gocommon.ExitWaitGroup.Add(1)
	go func() {
		defer gocommon.ExitWaitGroup.Done()
		<-quitChannel
		log.Info("http receive quit signal, http server close")
		httpServer.Close()
	}()

	log.Info("http listen and serve, url:http://127.0.0.1:", AppConfigSingleton.HttpPort)
	httpServer.ListenAndServe()
	log.Info("http server serve end")
}
