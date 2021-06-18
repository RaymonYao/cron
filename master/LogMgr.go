package master

import (
	"context"
	"crontab/common"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

// LogMgr mongodb日志管理
type LogMgr struct {
	client        *mongo.Client
	logCollection *mongo.Collection
}

var (
	GLogMgr *LogMgr
)

func InitLogMgr() (err error) {
	var (
		client *mongo.Client
	)

	//建立mongodb连接
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(GConfig.MongodbConnectTimeout)*time.Millisecond)
	defer cancel()
	client, err = mongo.Connect(ctx, options.Client().ApplyURI(GConfig.MongodbUri))

	GLogMgr = &LogMgr{
		client:        client,
		logCollection: client.Database("cron").Collection("log"),
	}
	return
}

// ListLog 查看任务日志
func (logMgr *LogMgr) ListLog(name string, skip int, limit int) (logArr []*common.JobLog, err error) {
	var (
		filter  *common.JobLogFilter
		logSort *common.SortLogByStartTime
		cursor  *mongo.Cursor
		jobLog  *common.JobLog
	)

	logArr = make([]*common.JobLog, 0)

	//过滤条件
	filter = &common.JobLogFilter{JobName: name}

	//按照任务开始时间倒排
	logSort = &common.SortLogByStartTime{SortOrder: -1}

	//查询
	if cursor, err = logMgr.logCollection.Find(
		context.TODO(),
		filter,
		options.Find().SetSort(logSort),
		options.Find().SetSkip(int64(skip)),
		options.Find().SetLimit(int64(limit))); err != nil {
		return
	}

	//延迟释放游标
	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()) {
		jobLog = &common.JobLog{}
		//反序列化BSON
		if err = cursor.Decode(jobLog); err != nil {
			continue //有日志不合法
		}
		logArr = append(logArr, jobLog)
	}
	return
}
