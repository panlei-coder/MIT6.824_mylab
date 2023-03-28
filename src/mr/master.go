package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// 定义了相应的常量，并用iota自增的枚举类型来初始化（0，1，2...）
const (
	Map = iota
	Reduce
	Sleep
)
const (
	Working = iota
	Timeout
)
const (
	NotStarted = iota
	Processing
	Finished
)

// Task结构体，表示当前正在执行的任务
type Task struct {
	Name      string // 任务名字
	Type      int    // 任务类别
	Status    int    // 任务状态：正常/超时
	mFileName string // 如果是map任务，则记录分配给该任务的文件名字
	rFileName int    //如果是reduce任务，则记录分配给该任务的文件组编号
}

// 一个全局递增变量，作为每个Task的名字，用来区分不同的Task，初始化为0
var taskNumber int = 0

// master结构体
type Master struct {
	// Your definitions here.
	mrecord      map[string]int   // 记录需要map的文件，0：未执行，1：正在执行，2：已完成
	rrecord      map[int]int      // 记录需要reduce的文件，0：未执行，1：正在执行，2：已完成
	reducefile   map[int][]string // 记录中间文件（key:int类型的文件组编号，value:string类型的文件名数组）
	taskmap      map[string]*Task // 任务池，记录当前正在执行的任务
	mcount       int              // 记录已经完成map的任务数量
	rcount       int              // 记录已经完成的reduce的任务数量
	mapFinished  bool             // 标志map任务是否已经完成
	reduceNumber int              // 需要执行的reduce的数量
	mutex        sync.Mutex       // 锁
}

// Your code here -- RPC handlers for the worker to call.

// 进行任务超时处理，对于超时的任务重新更新为未执行的状态
func (m *Master) HandleTimeout(taskName string) {
	// 休眠10秒
	time.Sleep(time.Second * 10)
	// 加锁
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if t, ok := m.taskmap[taskName]; ok {
		// 将当前任务设置为超时
		t.Status = Timeout

		// 如果任务是map类型
		if t.Type == Map {
			f := t.mFileName
			// 如果该任务是正在执行的，需要重新设置为未执行的状态
			if m.mrecord[f] == Processing {
				m.mrecord[f] = NotStarted
			}
		} else if t.Type == Reduce {
			f := t.rFileName
			// 如果该任务是正在执行的，需要重新设置为未执行的状态
			if m.rrecord[f] == Processing {
				m.rrecord[f] = NotStarted
			}
		}
	}
}

// master针对worker对象上报的请求进行回复
func (m *Master) Report(args *ReportStatusRequest, reply *ReportStatusResponse) error {
	reply.X = 1
	// 加锁
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// 如果任务还在任务池中
	if t, ok := m.taskmap[args.TaskName]; ok {
		flag := t.Status

		// 判断任务当前的状态进行不同的处理

		// 第一种情况：任务超时了，则直接将任务从任务池中剔除
		if flag == Timeout {
			delete(m.taskmap, args.TaskName)
			return nil
		}

		// 第二种情况：任务没有超时，根据任务的类型进行不同的处理
		ttype := t.Type
		// 如果是Map任务
		if ttype == Map {
			mf := t.mFileName
			m.mrecord[mf] = Finished
			m.mcount += 1
			// 判断当前所有的Map任务是否都已经完成了
			if m.mcount == len(m.mrecord) {
				m.mapFinished = true
			}
			// 需要将已完成的Map任务根据对应的任务编号添加到reducefile中
			for _, v := range args.FileName {
				index := strings.LastIndex(v, "_")
				num, err := strconv.Atoi(v[index+1:])
				if err != nil {
					log.Fatal(err)
				}
				m.reducefile[num] = append(m.reducefile[num], v)
			}
			// 将该任务从taskMap正在执行的任务中剔除
			delete(m.taskmap, t.Name)
			return nil
		} else if ttype == Reduce { // 如果是Reduce任务
			rf := t.rFileName
			m.rrecord[rf] = Finished
			m.rcount += 1
			delete(m.taskmap, t.Name)
			return nil
		} else {
			log.Fatal("task type is not map and reduce")
		}
	}

	message := fmt.Sprintf("%s task is not int Master record", args.TaskName)
	log.Println(message)
	return nil
}

// worker向master请求任务，master根据请求分配任务给worker
func (m *Master) GetTask(args *GetTaskRequest, reply *GetTaskResponse) error {
	// 加锁
	m.mutex.Lock()
	defer m.mutex.Unlock()

	reply.RFileName = make([]string, 0)
	reply.ReduceNumber = m.reduceNumber
	reply.MFileName = ""
	reply.TaskName = strconv.Itoa(taskNumber)
	taskNumber += 1

	// 如果map任务都已经完成了，则直接分配Reduce任务
	if m.mapFinished {
		for v := range m.rrecord {
			flag := m.rrecord[v]

			// 如果当前任务正在执行或者已完成，则去判断下一个任务
			if flag == Processing || flag == Finished {
				continue
			} else {
				m.rrecord[v] = Processing
				for _, fileName := range m.reducefile[v] {
					reply.RFileName = append(reply.RFileName, fileName)
				}
				reply.TaskType = Reduce
				// 创建一个task任务，并添加到taskmap中
				t := &Task{reply.TaskName, reply.TaskType, Working, "", v}
				m.taskmap[reply.TaskName] = t
				// 启动一个协程，当任务超时时进行相应的处理
				go m.HandleTimeout(reply.TaskName)
				return nil
			}
		}

		// 如果当前没有任务可进行分配，则将状态设置为Sleep
		reply.TaskType = Sleep
		return nil
	} else {
		// 分配map任务
		for v, _ := range m.mrecord {
			flag := m.mrecord[v]

			// 如果当前任务正在执行或者已经完成，则直接跳过进行下一个
			if flag == Processing || flag == Finished {
				continue
			} else {
				// 修改记录的状态
				m.mrecord[v] = Processing
				reply.MFileName = v
				reply.TaskType = Map
				// 创建task任务，并添加到taskmap中
				t := &Task{reply.TaskName, reply.TaskType, Working, reply.MFileName, -1}
				m.taskmap[reply.TaskName] = t
				//启动协程，针对超时情况进行相应的处理
				go m.HandleTimeout(reply.TaskName)
				return nil
			}
		}

		reply.TaskType = Sleep
		return nil
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	// 将对象m注册到远程服务RPC中，以便客户端可以调用这个服务
	rpc.Register(m)
	// 注册了RPC处理程序，以便可以通过HTTP协议来处理客户端请求
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	// 删除之前生成的套接字文件
	os.Remove(sockname)
	// 创建一个Unix域上的套接字，并在该套接字上监听来自客户端的请求
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	// 启动http服务，并监听来自客户端的socket请求
	log.Println("listen successed")
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	// 判断是否已经全部完成
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.rcount == m.reduceNumber {
		ret = true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
// files为文件名数组，nReduce用户指定的Reduce的数量
func MakeMaster(files []string, nReduce int) *Master {
	// 初始化
	m := Master{
		mrecord:      make(map[string]int),
		rrecord:      make(map[int]int),
		reducefile:   make(map[int][]string),
		taskmap:      make(map[string]*Task),
		mcount:       0,
		rcount:       0,
		mapFinished:  false,
		reduceNumber: nReduce,
		mutex:        sync.Mutex{},
	}

	// Your code here.

	for _, f := range files {
		m.mrecord[f] = 0
	}

	for i := 0; i < nReduce; i++ {
		m.rrecord[i] = 0
	}

	m.server()
	return &m
}
