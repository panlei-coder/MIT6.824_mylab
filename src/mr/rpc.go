package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// 获取任务的请求结构体，本身不用携带信息
type GetTaskRequest struct {
	X int
}

// master对于任务请求RPC的回复
type GetTaskResponse struct {
	MFileName    string   // 如果是map任务，则记录map文件名字
	TaskName     string   // 该任务的名字（一个全局唯一的编号）
	RFileName    []string // 如果是reduce任务，则记录reduce文件名字
	TaskType     int      // 任务类别，0：map任务，1：reduce任务，2：sleep
	ReduceNumber int      // 需要将中间文件分组的数量（即考虑要分给多少个reduce worker）
}

// worker上报任务完成状态
type ReportStatusRequest struct {
	FileName []string // 如果是map任务，需要告知master中间文件的信息
	TaskName string   // 该任务的名字
}

// master对上报任务的回复，不需要携带信息
type ReportStatusResponse struct {
	X int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid()) // 获取当前用户的UID
	return s
}
