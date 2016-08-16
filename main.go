// workpool project main.go
package main

import (
	"fmt"
	"runtime"
	"time"
)

// 声明函数类型
type taskFuncType func() error
type commitFuncType func()
type rollbackFuncType func()

// 定义任务
type Task struct {
	taskID           int              // 任务编号
	taskFunc         taskFuncType     // 任务函数
	transactionID    int              // 事务编号，多数据库并行操作的属于同一个事务
	totalShardingNum int              // 并行操作数据库个数，从1开始，1表示只操作单数据库，此时后续变量无效
	shardingID       int              // 当前任务处理的是第几个数据库，0表示主控任务而不是工作任务
	commitFunc       commitFuncType   // 全部成功，commit函数
	rollbackFunc     rollbackFuncType // 部分失败，超时等情况，rollback函数
	w2cSyncChan      chan bool        // 工作线程给主控线程发送任务执行结果的通知
	c2wSyncChan      chan bool        // 主控线程给工作线程发送统一commit和rollback通知
}

// 构造单数据库任务
func (self *Task) genSingleDBTask(taskID int, taskFunc taskFuncType, transactionID int) {
	self.taskID = taskID
	self.taskFunc = taskFunc
	self.transactionID = transactionID
	self.totalShardingNum = 1
	self.shardingID = 0
}

// 构造多数据库主控任务
func (self *Task) genMultiDBMainTask(taskID int, taskFunc taskFuncType, transactionID int,
	totalShardingNum int, commitFunc commitFuncType, rollbackFunc rollbackFuncType) (chan bool, chan bool) {
	self.taskID = taskID
	self.taskFunc = taskFunc
	self.transactionID = transactionID
	self.totalShardingNum = totalShardingNum
	self.shardingID = 0
	self.commitFunc = commitFunc
	self.rollbackFunc = rollbackFunc
	self.w2cSyncChan = make(chan bool, totalShardingNum)
	self.c2wSyncChan = make(chan bool, totalShardingNum)
	return self.w2cSyncChan, self.c2wSyncChan
}

// 构造多数据库工作任务
func (self *Task) genMultiDBWorkTask(taskID int, taskFunc taskFuncType, transactionID int,
	totalShardingNum int, shardingID int, commitFunc commitFuncType, rollbackFunc rollbackFuncType,
	w2cSyncChan chan bool, c2wSyncChan chan bool) {
	self.taskID = taskID
	self.taskFunc = taskFunc
	self.transactionID = transactionID
	self.totalShardingNum = totalShardingNum
	self.shardingID = shardingID
	self.commitFunc = commitFunc
	self.rollbackFunc = rollbackFunc
	self.w2cSyncChan = w2cSyncChan
	self.c2wSyncChan = c2wSyncChan
}

// 任务执行后返回信息
type TaskResult struct {
	workerID         int   // 运行该任务的线程ID
	taskID           int   // 任务ID
	transactionID    int   // 事务ID
	totalShardingNum int   // 并行操作数据库个数，从1开始，1表示只操作单数据库，此时后续变量无效
	shardingID       int   // 当前任务处理的是第几个数据库，0表示主控任务而不是工作任务
	err              error // 错误描述信息
}

// 工作线程池
type WorkPool struct {
	// 任务队列
	taskQueue chan Task
	// 线程并发数
	numofWork int
	// 队列中任务最大数
	numofTask int
	// 任务执行结果队列
	taskResultQueue chan TaskResult
}

// 初始化工作线程池
func (self *WorkPool) init(numofWork int, numofTask int) {
	self.taskQueue = make(chan Task, numofTask)
	self.numofWork = numofWork
	self.numofTask = numofTask
	self.taskResultQueue = make(chan TaskResult, numofTask)
}

// 将任务添加到线程池
func (self *WorkPool) addTask(task Task) {
	self.taskQueue <- task
}

// 测试用，关闭channel
func (self *WorkPool) closeChannel() {
	close(self.taskQueue)
	close(self.taskResultQueue)
}

// 线程池运行
func (self *WorkPool) startWork() {
	fmt.Println("WorkPool.startWork()")
	// 启动指定数目的协程并发执行
	for i := 0; i < self.numofWork; i++ {
		go func(workerID int) {
			for task := range self.taskQueue {
				// 多数据库并发
				if task.totalShardingNum > 1 {
					// 主控任务
					if task.shardingID == 0 {
						fmt.Printf("Worker %d, taskID %d, transactionID %d, totalShardingNum %d, "+
							"shardingID %d, main task.\n", workerID, task.taskID,
							task.transactionID, task.totalShardingNum, task.shardingID)

						statResult := true
						var statErr error = nil
						for m := 0; m < task.totalShardingNum; m++ {
							select {
							case workResult, ok := <-task.w2cSyncChan:
								if ok == false || workResult == false {
									statResult = false
									statErr = fmt.Errorf("In worker %d, taskID %d, transactionID %d, "+
										"main task get false from work task.\n", workerID, task.taskID,
										task.transactionID)
								}
							case <-time.After(time.Second * 10):
								statResult = false
								statErr = fmt.Errorf("In worker %d, taskID %d, transactionID %d, "+
									"main task time out.\n", workerID, task.taskID, task.transactionID)
							}
						}

						for n := 0; n < task.totalShardingNum; n++ {
							task.c2wSyncChan <- statResult
						}

						self.taskResultQueue <- TaskResult{workerID, task.taskID, task.transactionID,
							task.totalShardingNum, task.shardingID, statErr}
					} else { // 工作任务
						fmt.Printf("Worker %d, taskID %d, transactionID %d, totalShardingNum %d, "+
							"shardingID %d, work task.\n", workerID, task.taskID,
							task.transactionID, task.totalShardingNum, task.shardingID)

						var taskRes bool = true
						err := task.taskFunc()
						if err == nil {
							taskRes = true
						} else {
							taskRes = false
						}

						task.w2cSyncChan <- taskRes

						var statErr error = nil
						select {
						case controlResult, ok := <-task.c2wSyncChan:
							if ok == false || controlResult == false {
								statErr = fmt.Errorf("In worker %d, taskID %d, transactionID %d, totalShardingNum %d, "+
									"shardingID %d, work task get false from main task.\n",
									workerID, task.taskID, task.transactionID, task.totalShardingNum, task.shardingID)
							}
						case <-time.After(time.Second * 10):
							statErr = fmt.Errorf("In worker %d, taskID %d, transactionID %d, totalShardingNum %d, "+
								"shardingID %d, work task time out.\n",
								workerID, task.taskID, task.transactionID, task.totalShardingNum, task.shardingID)
						}

						if statErr == nil && err == nil {
							task.commitFunc()
							self.taskResultQueue <- TaskResult{workerID, task.taskID, task.transactionID,
								task.totalShardingNum, task.shardingID, nil}
						} else if err == nil && statErr != nil {
							task.rollbackFunc()
							self.taskResultQueue <- TaskResult{workerID, task.taskID, task.transactionID,
								task.totalShardingNum, task.shardingID, statErr}
						} else {
							self.taskResultQueue <- TaskResult{workerID, task.taskID, task.transactionID,
								task.totalShardingNum, task.shardingID, err}
						}
					}
				} else { // 单数据分区
					fmt.Printf("Worker %d, taskID %d, transactionID %d, totalShardingNum %d, "+
						"shardingID %d, single db task.\n", workerID, task.taskID,
						task.transactionID, task.totalShardingNum, task.shardingID)

					err := task.taskFunc()
					self.taskResultQueue <- TaskResult{workerID, task.taskID, task.transactionID,
						task.totalShardingNum, task.shardingID, err}
				}
			}
		}(i)
	}

	// 处理任务的返回值
	go func() {
		for taskResult := range self.taskResultQueue {
			if taskResult.err == nil {
				fmt.Printf("workerID %d, taskID %d, transactionID %d, totalShardingNum %d, "+
					"shardingID %d, return true.\n", taskResult.workerID, taskResult.taskID,
					taskResult.transactionID, taskResult.totalShardingNum, taskResult.shardingID)
			} else {
				fmt.Printf("workerID %d, taskID %d, transactionID %d, totalShardingNum %d, "+
					"shardingID %d, %s.\n", taskResult.workerID, taskResult.taskID,
					taskResult.transactionID, taskResult.totalShardingNum, taskResult.shardingID,
					taskResult.err.Error())
			}
		}
	}()
}

// 模拟数据库操作
func dbOperate(workSQL string) error {
	fmt.Printf("Doing work, workSQL %s\n", workSQL)
	time.Sleep(time.Second * 2)
	return nil
}

// 任务函数
func taskFunc() error {
	return dbOperate("select * from table;")
}

// commit函数
func commit() {
	fmt.Println("Doing commit.")
}

// rollback函数
func rollback() {
	fmt.Println("Doing rollback.")
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	fmt.Println("The Begin!")
	workPool := new(WorkPool)
	workPool.init(8, 100)
	workPool.startWork()

	// 添加任务
	var task1 Task
	w2cSyncChan, c2wSyncChan := task1.genMultiDBMainTask(1, taskFunc, 1, 3, commit, rollback)
	workPool.addTask(task1)

	var task2 Task
	task2.genMultiDBWorkTask(2, taskFunc, 1, 3, 1, commit, rollback, w2cSyncChan, c2wSyncChan)
	workPool.addTask(task2)

	var task3 Task
	task3.genMultiDBWorkTask(3, taskFunc, 1, 3, 2, commit, rollback, w2cSyncChan, c2wSyncChan)
	workPool.addTask(task3)

	var task4 Task
	task4.genMultiDBWorkTask(4, taskFunc, 1, 3, 3, commit, rollback, w2cSyncChan, c2wSyncChan)
	workPool.addTask(task4)

	time.Sleep(20 * time.Second)
	fmt.Println("WorkPool.closeChannel()")
	workPool.closeChannel()

	time.Sleep(3 * time.Second)

	fmt.Println("The End!")
}
