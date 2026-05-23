package main

import (
	"context"
	"flag"
	"fmt"
	"pkg/config"
	"pkg/temporal"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

var (
	configPath string
)

func parceFlags() {
	flag.StringVar(&configPath, "config", "", "path to config file")
	flag.Parse()
}

type Config struct {
	Temporal temporal.TemporalConfig
}

func main() {
	parceFlags()
	if configPath == "" {
		fmt.Println("config path is required")
		return
	}

	conf := Config{}
	err := config.LoadConfigFromFile(configPath, &conf)
	if err != nil {
		fmt.Println("Failed to load temporal config")
		return
	}

	fmt.Println(configPath)
	fmt.Println(conf)

	cli, err := temporal.NewTemporalClient(&conf.Temporal)
	if err != nil {
		fmt.Println("Failed to init temporal client", err)
		return
	}

	fmt.Println("Successfully connected!")

	w := worker.New(cli, "test-queue", worker.Options{})
	w.RegisterActivity(HelloActivity)
	w.RegisterWorkflow(HelloWorkflow)

	ctx := context.Background()
	stopCh := make(chan interface{})
	go func() {
		<-ctx.Done()
		close(stopCh)
	}()

	if err := w.Run(stopCh); err != nil {
		fmt.Println("Failed to run worker")
		return
	}

}

func HelloWorkflow(ctx workflow.Context, name string) (string, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Started HelloWorkflow", "name", name)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Second * 5,
	})

	var res string
	if err := workflow.ExecuteActivity(ctx, HelloActivity, name).Get(ctx, &res); err != nil {
		logger.Error("Failed to execute activity", "error", err)
		return "", err
	}

	return res, nil
}

func HelloActivity(ctx context.Context, name string) (string, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Started HelloActivity", "name", name)

	return "Hello " + name + "!", nil
}
