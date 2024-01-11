package main

import (
	"fmt"
	"os"

	"github.com/noneback/llogtail"
	"github.com/op/go-logging"
)

func main() {
	// simple implementation
	args := os.Args[1:]
	if len(args) < 1 {
		fmt.Println("conf missing")
		os.Exit(1)
	}
	llogtail.InitLogger(&llogtail.LogOption{
		Verbose: true, Level: logging.DEBUG,
	})

	cpath := args[0]
	conf, err := llogtail.ReadLogCollectorConf(cpath)
	if err != nil {
		fmt.Println("read conf failed: ", err)
		os.Exit(1)
	}
	fmt.Printf("%+v\n", conf)

	lc := llogtail.NewLogCollector()
	if err = lc.Init(*conf); err != nil {
		fmt.Println("collector init failed: ", err)
		os.Exit(1)
	}

	lc.Run()

	defer lc.Close()
	// 创建系统信号接收器
	signals := make(chan os.Signal, 0)

	// 阻塞直到收到信号
	sig := <-signals
	fmt.Printf("Received signal: %s, exiting...\n", sig)
}
