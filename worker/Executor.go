package worker

import (
	"crontab/common"
	"math/rand"
	"os/exec"
	"time"
)

// Executor 任务执行器
type Executor struct {
}

var (
	GExecutor *Executor
)

// ExecuteJob 执行一个任务
func (executor *Executor) ExecuteJob(info *common.JobExecuteInfo) {
	go func() {
		var (
			cmd     *exec.Cmd
			err     error
			output  []byte
			result  *common.JobExecuteResult
			jobLock *JobLock
		)

		//任务结果
		result = &common.JobExecuteResult{
			ExecuteInfo: info,
			Output:      make([]byte, 0),
		}

		//初始化分布式锁
		jobLock = GJobMgr.CreateJobLock(info.Job.Name)

		//记录任务开始时间
		result.StartTime = time.Now()

		//上锁
		//随机睡眠(0-1s) 为了防止各个服务器时间不准导致的抢锁不公平，正常情况下各个服务器会用ntp时间服务器进行时间同步
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)

		err = jobLock.TryLock()
		defer jobLock.Unlock()

		if err != nil {
			//上锁失败
			result.Err = err
			result.EndTime = time.Now()
		} else {
			//上锁成功后，重置任务启动时间
			result.StartTime = time.Now()

			//执行shell命令
			cmd = exec.CommandContext(info.CancelCtx, "/bin/bash", "-c", info.Job.Command)

			//执行并捕获输出
			output, err = cmd.CombinedOutput()

			//记录任务结束时间
			result.EndTime = time.Now()
			result.Output = output
			result.Err = err
		}
		//任务执行完成后，把执行的结果返回给Scheduler,Scheduler会从executingTable中删除掉执行记录
		GScheduler.PushJobResult(result)
	}()
}

// InitExecutor 初始化执行器
func InitExecutor() (err error) {
	GExecutor = &Executor{}
	return
}
