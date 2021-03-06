package worker

import (
	"context"
	"crontab/common"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

// LogSink mongodb存储日志
type LogSink struct {
	client         *mongo.Client
	logCollection  *mongo.Collection
	logChan        chan *common.JobLog
	autoCommitChan chan *common.LogBatch
}

var (
	// GLogSink 单例
	GLogSink *LogSink
)

//批量写入日志
func (logSink *LogSink) saveLogs(batch *common.LogBatch) {
	logSink.logCollection.InsertMany(context.TODO(), batch.Logs)
}

//日志存储协程
func (logSink *LogSink) writeLoop() {
	var (
		log          *common.JobLog
		logBatch     *common.LogBatch //当前的批次
		commitTimer  *time.Timer
		timeoutBatch *common.LogBatch //超时批次
	)

	for {
		select {
		case log = <-logSink.logChan:
			if logBatch == nil {
				logBatch = &common.LogBatch{}
				//让这个批次超时自动提交(给1秒的时间)
				commitTimer = time.AfterFunc(
					time.Duration(GConfig.JobLogCommitTimeout)*time.Millisecond,
					//1.闭包写法，为了batch变量不受外面污染
					//2.之所以要autoCommitChan是因为要串行化(因为是for循环)，避免并发，writeLoop协程在提交数据，定时器也在提交数据，容易并发操作，
					//定义autoCommitChan协程可以解决这个问题，这里仅仅是发出超时通知，不要直接提交数据
					func(batch *common.LogBatch) func() {
						return func() {
							logSink.autoCommitChan <- batch
						}
					}(logBatch),
				)
			}

			//把新日志追加到批次中
			logBatch.Logs = append(logBatch.Logs, log)

			//如果批次满了，就立即发送
			if len(logBatch.Logs) >= GConfig.JobLogBatchSize {
				//发送日志
				logSink.saveLogs(logBatch)
				//清空logBatch
				logBatch = nil
				//取消定时器
				commitTimer.Stop()
			}
		case timeoutBatch = <-logSink.autoCommitChan:
			//过期的批次
			//判断过期批次是否仍旧是当前的批次
			if timeoutBatch != logBatch {
				//跳过已经被提交的批次
				continue
			}
			//把批次写入到mongo中
			logSink.saveLogs(timeoutBatch)
			//清空logBatch
			logBatch = nil
		}
	}
}

func InitLogSink() (err error) {
	var (
		client *mongo.Client
	)

	//建立mongodb连接
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(GConfig.MongodbConnectTimeout)*time.Millisecond)
	defer cancel()
	client, err = mongo.Connect(ctx, options.Client().ApplyURI(GConfig.MongodbUri))
	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()

	//选择db和collection
	GLogSink = &LogSink{
		client:         client,
		logCollection:  client.Database("cron").Collection("log"),
		logChan:        make(chan *common.JobLog, 1000),
		autoCommitChan: make(chan *common.LogBatch, 1000),
	}

	//启动一个mongodb处理协程
	go GLogSink.writeLoop()
	return
}

// Append 发送日志
func (logSink *LogSink) Append(jobLog *common.JobLog) {
	select {
	case logSink.logChan <- jobLog:
	default:
		// 队列满了就丢弃
	}
}
