package mr

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
	Files []string
	PendingFiles []string
	ReduceTCount int
	WorkerCount int
	ProcessedCount int
	ProcessedMap map[string]bool
	FailedFiles map[string]int
	wm sync.Mutex

}

// Your code here -- RPC handlers for the worker to call.

//MapReq is an RPC call made by the worker to request for a task
func (m *Master) WorkerReq(args *WorkerArgs,reply *WorkerReply) error {
	file,w,err := m.fetchUnprocessedFile(args.WokerName)
	if err!=nil {
		return err
	}
	fileStream := make([]string,0)
	fileStream = append(fileStream,file)
	reply.Files = fileStream
	reply.ReduceTCount = m.ReduceTCount
	reply.Worker = w
	fmt.Printf("Sending file %s for worker %d \n",file,w)
	return nil
}

func (m *Master) ReduceReq(args *ReduceArgs,reply *ReduceReply) error {
	return nil
}

func (m *Master) WorkerNotify(args *NotifyArgs,reply *NotifyReply) error {
	fn := args.FilePath
	w := args.Worker
	fmt.Printf("Got Notified for file %s from worker %d",fn,w)
	m.wm.Lock()
	m.ProcessedMap[fn] = true
	m.ProcessedCount = m.ProcessedCount -1
	m.wm.Unlock()
	return nil
}

func (m *Master) ReduceNotify(args *NotifyArgs,reply *NotifyReply) error {
	return nil
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
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	m.wm.Lock()
	if   m.ProcessedCount ==0 && len(m.PendingFiles)==0 {
		ret = true
	}else{
		//fmt.Println("Pending Files Count:",len(m.PendingFiles))
		//fmt.Println("Processed Files Count:",m.ProcessedCount)
	}
	m.wm.Unlock()
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		Files: files,
		PendingFiles: files,
		ReduceTCount:nReduce,
		ProcessedMap:make(map[string]bool),
		ProcessedCount:3,
		FailedFiles:make(map[string]int),
		wm : sync.Mutex{},
	}

	for _,f := range m.Files{
		m.ProcessedMap[f] = false
	}

	fmt.Println("Total Number of processed count:",m.ProcessedCount)
	m.server()
	return &m
}


func (m *Master) checkForTaskTimeout(file string,done <-chan interface{},ticker *time.Ticker) {

	for{
		select {
			case <-done:
				return
			case <-ticker.C:
				m.wm.Lock()
				if !m.ProcessedMap[file] {
					m.FailedFiles[file] = m.FailedFiles[file]+1
					m.PendingFiles = append(m.PendingFiles,file)
					if m.FailedFiles[file] < 4 {
						m.ProcessedMap[file] = false
					}else{
						m.ProcessedMap[file]=true
					}
				}
				m.wm.Unlock()
		}
	}
}

func (m *Master) fetchUnprocessedFile(w int) (fn string,worker int, err error) {
	m.wm.Lock()
	defer m.wm.Unlock()
	if len(m.PendingFiles)>0 {
		fmt.Printf("Started Processing worker %d\n",w)
		fn = m.PendingFiles[0]
		m.PendingFiles = m.PendingFiles[1:]
		m.WorkerCount = w
		//     m.ProcessedCount = m.ProcessedCount+1
		return fn,m.WorkerCount,nil
	}else{
		return "",-1,errors.New("Please Exit")
	}
}
