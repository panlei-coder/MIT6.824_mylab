package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// 处理Map任务的方法
func HandleMap(mapf func(string, string) []KeyValue, filename string, filenum int, tasknum string) []string {
	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		message := fmt.Sprintf("cannot open %v", filename)
		log.Fatal(message)
	}

	// 从file中将所有的内容读取出来
	content, err := ioutil.ReadAll(file)
	// log.Println(content)
	if err != nil {
		message := fmt.Sprintf("cannot read %v", filename)
		log.Fatal(message)
	}
	file.Close()

	// 利用mapf函数处理filename文件中的content的内容并生成切片
	// append中"..."是将kva中的元素逐个加入到intermediate中
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	// 根据filenum创建相应数量大小的文件指针数组
	filenames := make([]string, filenum)
	files := make([]*os.File, filenum)

	// 将对filenames和files进行赋值操作
	for i := 0; i < filenum; i++ {
		oname := "mr"
		oname = oname + "_" + tasknum + "_" + strconv.Itoa(i)
		log.Println("create ", oname)

		ofile, _ := os.Create(oname)
		files[i] = ofile
		filenames[i] = oname
	}

	// 将intermediate根据hash索引放入到对应的file文件中
	for _, kv := range intermediate {
		index := ihash(kv.Key) % filenum
		enc := json.NewEncoder(files[index])
		enc.Encode(&kv)
	}

	return filenames
}

// 处理Reduce任务的方法
func HandleReduce(reducef func(string, []string) string, filenames []string) string {
	// 创建len(filenames)个文件指针的数组
	files := make([]*os.File, len(filenames))
	intermediate := []KeyValue{}
	for i := 0; i < len(filenames); i++ {
		files[i], _ = os.Open(filenames[i])
		kv := KeyValue{}
		// 创建一个*json.Decode对象，并将files[i]的对象和它关联起来
		// 通过dec.Decode获取下一个键值对，并存放到kv中
		dec := json.NewDecoder(files[i])
		for {
			// 如果出现解码错误或者是读到了文件尾，err会不等于nil(空指针)
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	// 对intermediate进行排序
	sort.Sort(ByKey(intermediate))
	message := fmt.Sprintf("intermediate:%d", len(intermediate))
	log.Println(message)
	oname := "mr-out-"

	index := filenames[0][strings.LastIndex(filenames[0], "_")+1:]
	oname = oname + index
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// 将reduce的结果输出到ofile文件中
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	return oname
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	for {
		// 获取任务
		args := GetTaskRequest{}
		args.X = 0
		rep := GetTaskResponse{}
		call("Master.GetTask", &args, &rep)
		log.Println("name:", rep.TaskName, "type:", rep.TaskType)

		// 根据获取任务的类型进行相应的处理
		if rep.TaskType == Map {
			filenames := HandleMap(mapf, rep.MFileName, rep.ReduceNumber, rep.TaskName)
			rargs := ReportStatusRequest{}
			rargs.TaskName = rep.TaskName
			rargs.FileName = filenames
			rreply := ReportStatusResponse{}
			rreply.X = 0
			call("Master.Report", &rargs, &rreply)
		} else if rep.TaskType == Reduce {
			HandleReduce(reducef, rep.RFileName)
			rargs := ReportStatusRequest{}
			rargs.TaskName = rep.TaskName
			rargs.FileName = make([]string, 0)
			rreply := ReportStatusResponse{}
			rreply.X = 0
			call("Master.Report", &rargs, &rreply)
		} else if rep.TaskType == Sleep {
			time.Sleep(time.Millisecond * 10)
			log.Println("Sleep Task")
		} else {
			log.Fatal("get task is not map,reduce and sleep")
		}
	}

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// 通过rpc.DialHTTP远程连接RPC服务端，并通过c.Call调用远程方法
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
