package llogtail

// func TestLogCollector(t *testing.T) {
// 	Pre()
// 	InitLogger(&defauleLogOption)
// 	collector := NewLogCollector(
// 		&LogWatcherOption{
// 			FilterInterval: 1,
// 			BufferSize: 1024,
// 		},
// 	)
// 	conf := &LogConf {
// 		Dir: "./tests",
// 		Pattern: "*.log",
// 		LineSep: "\n",
// 	}

// 	t.Run("Test Collector Init",func(t *testing.T) {
// 		if err := collector.Init(conf);err != nil {
// 			logger.Error("Init -> %w",err)
// 			t.FailNow()
// 		}
// 	})
// 	defer collector.Close()

// 	t.Run("Test Collector Collect Once",func(t *testing.T) {
// 		collector.Run()
// 		for _, log := range kLogs {
// 			modify(log, kDataOneKB)
// 		}
// 		time.Sleep(time.Millisecond * 3000)

// 		content, err:=collector.Collect()
// 		if err != nil {
// 			logger.Errorf("Collect -> %w", err)
// 			t.FailNow()
// 		}
// 		logger.Info(content)
// 	})

// }