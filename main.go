package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hstreamdb/hstreamdb-go/hstream"
	"github.com/hstreamdb/hstreamdb-go/util"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

type GlobalFlags struct {
	Address      string
	Verbose      bool
	Intervals    int32
	StreamName   string
	SubId        string
	ConsumerName string
	AckTimeout   int32
	Wait         int32
}

var globalFlags = GlobalFlags{}

func initFlags(rootCmd *cobra.Command) {
	rootCmd.PersistentFlags().StringVarP(&globalFlags.Address, "host", "p", "127.0.0.1:6570",
		"address of the HStream-server, separated by commas. e.g. 127.0.0.1:6570,127.0.0.2:6570,127.0.0.3:6570")
	rootCmd.PersistentFlags().BoolVarP(&globalFlags.Verbose, "verbose", "v", false, "print every fetched records if true.")
	rootCmd.PersistentFlags().Int32VarP(&globalFlags.Intervals, "interval", "i", 3, "report statistical information in seconds. intervals less than and equal to 0 means report after all done.")
	rootCmd.PersistentFlags().StringVarP(&globalFlags.StreamName, "stream-name", "n", "", "name of stream to subscribe")
	rootCmd.MarkPersistentFlagRequired("stream-name")
	rootCmd.PersistentFlags().StringVarP(&globalFlags.SubId, "subscription-id", "s", "", "subscription id.")
	rootCmd.MarkPersistentFlagRequired("subscription-id")
	rootCmd.PersistentFlags().StringVarP(&globalFlags.ConsumerName, "consumer-name", "c", "", "consumer name.")
	rootCmd.PersistentFlags().Int32VarP(&globalFlags.AckTimeout, "ack-timeout", "t", 60, "ack timeout in seconds.")
	rootCmd.PersistentFlags().Int32VarP(&globalFlags.Wait, "wait", "w", 60, "if no more data fetched in wait seconds, then terminate.")
}

func fetch(cmd *cobra.Command, args []string) error {
	client, err := hstream.NewHStreamClient(globalFlags.Address)
	if err != nil {
		util.Logger().Error("failed to create hstream client", zap.Error(err))
		return err
	}
	defer client.Close()

	if err = client.CreateSubscription(globalFlags.SubId, globalFlags.StreamName, globalFlags.AckTimeout); err != nil {
		util.Logger().Error("create subscription err", zap.Error(err))
		return err
	}
	defer func() {
		client.DeleteSubscription(globalFlags.SubId, true)
	}()

	if globalFlags.ConsumerName == "" {
		globalFlags.ConsumerName = fmt.Sprintf("consumer_%d", time.Now().UnixNano())
	}
	consumer := client.NewConsumer(globalFlags.ConsumerName, globalFlags.SubId)
	defer consumer.Stop()

	successRead := int64(0)
	failedRead := int64(0)
	ctx := registerSignalHandler()

	go func() {
		if globalFlags.Intervals <= 0 || globalFlags.Verbose {
			return
		}

		duration := time.Duration(globalFlags.Intervals) * time.Second
		timer := time.NewTimer(duration)
		defer func() {
			if !timer.Stop() {
				<-timer.C
			}
		}()

		lastSuccessRead := int64(0)
		lastFailedRead := int64(0)
		for {
			select {
			case <-timer.C:
				sRead := atomic.LoadInt64(&successRead)
				fRead := atomic.LoadInt64(&failedRead)
				success := sRead - lastSuccessRead
				failed := fRead - lastFailedRead
				util.Logger().Info(fmt.Sprintf("[%s]", globalFlags.SubId),
					zap.Int64("success read", success), zap.Int64("failed read", failed))
				lastSuccessRead = sRead
				lastFailedRead = fRead
				timer.Reset(duration)
			case <-ctx.Done():
				return
			}
		}
	}()

	dataCh := consumer.StartFetch()
	//defer close(dataCh)

	waitTimeout := time.Duration(globalFlags.Wait) * time.Second
	latestActiveTs := int64(0)
	ticker := time.NewTicker(waitTimeout)
	defer ticker.Stop()

	for {
		select {
		case res, ok := <-dataCh:
			if !ok {
				util.Logger().Info("dataCh closed, stop fetch data.")
				return nil
			}
			atomic.StoreInt64(&latestActiveTs, time.Now().UnixNano())
			length := len(res.Result)
			if res.Err != nil {
				atomic.AddInt64(&failedRead, int64(length))
				continue
			}
			atomic.AddInt64(&successRead, int64(length))

			for _, record := range res.Result {
				record.Ack()
				if globalFlags.Verbose {
					rid := record.GetRecordId()
					recordType := record.GetRecordType()
					util.Logger().Info("recordType", zap.String("tp", recordType.String()))
					var payload string
					switch record.GetRecordType() {
					case hstream.RAWRECORD:
						p := record.GetPayload().([]byte)
						payload = string(p)
					case hstream.HRECORD:
						p := record.GetPayload().(map[string]interface{})
						payload = fmt.Sprintf("%+v", p)
					}
					util.Logger().Info(fmt.Sprintf("[%s]", globalFlags.SubId),
						zap.String("recordId", rid.String()), zap.String("payload", payload))
				}
			}
		case <-ticker.C:
			lastTs := atomic.LoadInt64(&latestActiveTs)
			now := time.Now().UnixNano()
			timeOutTs := waitTimeout.Nanoseconds()
			if now-lastTs >= timeOutTs {
				util.Logger().Info("no more data fetched in timeout duration, stop.")
				util.Logger().Info(fmt.Sprintf("[%s]", globalFlags.SubId),
					zap.Int64("total success read", atomic.LoadInt64(&successRead)),
					zap.Int64("total failed read", atomic.LoadInt64(&failedRead)))
				return nil
			} else {
				diff := timeOutTs - (now - lastTs)
				ticker.Reset(time.Duration(diff) * time.Nanosecond)
			}
		case <-ctx.Done():
			util.Logger().Info(fmt.Sprintf("[%s]", globalFlags.SubId),
				zap.Int64("total success read", atomic.LoadInt64(&successRead)),
				zap.Int64("total failed read", atomic.LoadInt64(&failedRead)))
			return nil
		}
	}
}

func registerSignalHandler() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan,
			syscall.SIGHUP,
			syscall.SIGINT,
			syscall.SIGTERM,
			syscall.SIGQUIT)
		sig := <-sigChan
		util.Logger().Info("signal received", zap.String("signal", sig.String()))
		cancel()
	}()
	return ctx
}

func main() {
	var rootCmd = &cobra.Command{
		Use:   "fetcher",
		Short: "fetcher is a command line tool for consuming data from HStreamDB Cluster.",
		RunE:  fetch,
	}
	logger, _ := util.InitLogger(util.INFO)
	util.ReplaceGlobals(logger)
	initFlags(rootCmd)
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
