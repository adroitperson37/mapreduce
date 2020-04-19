package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

//Master is used
type Master struct {
	Files           []string
	PendingFiles    []string
	MappedFiles     []string
	MappedFileMap   map[string]bool
	ReduceWorkers   []string
	MapWorkers      []int
	ReduceWorkerMap map[string]bool
	MapWorkerMap    map[int]bool
	IsWorker        bool
	IsReducer       bool
	ReduceTCount    int
	WorkerCount     int
	ProcessedCount  int
	ProcessedMap    map[string]bool
	FailedFiles     map[string]int
	wm              sync.Mutex
	cond            *sync.Cond
}

// Your code here -- RPC handlers for the worker to call.

//WorkerReq is an RPC call made by the worker to request for a task
func (m *Master) WorkerReq(args *WorkerArgs, reply *WorkerReply) error {
	file, w, err := m.fetchUnprocessedFile(args.WokerName)
	if err != nil {
		return err
	}
	fileStream := make([]string, 0)
	fileStream = append(fileStream, file)
	reply.Files = fileStream
	reply.ReduceTCount = m.ReduceTCount
	reply.Worker = w
	//fmt.Printf("Sending file %s for worker %d \n", file, w)
	return nil
}

//ReduceReq is an RPC call made by the worker to request for a task
func (m *Master) ReduceReq(args *ReduceArgs, reply *ReduceReply) error {

	file, err := m.fetchMappedFile(args)
	if err != nil {
		reply.Status = false
		return err
	}
	fileStream := make([]string, 0)
	fileStream = append(fileStream, file)
	reply.Files = fileStream
	reply.Status = true
	fmt.Printf("Sending file %s for worker \n", file)
	return nil
}

//WorkerNotify is an RPC call made by the worker to request for a task
func (m *Master) WorkerNotify(args *NotifyArgs, reply *NotifyReply) error {
	rd := args.Reducer
	//fmt.Printf("Got Notified from reducer %s", rd)
	m.wm.Lock()
	m.ProcessedMap[rd] = true
	if len(m.ReduceWorkers) > 0 {
		m.ReduceWorkers = m.ReduceWorkers[1:]
	}
	//m.ProcessedCount = m.ProcessedCount - 1

	m.wm.Unlock()
	return nil
}

//MapNotify is an RPC call made by the worker to request for a task
func (m *Master) MapNotify(args *MapNotifyArgs, reply *MapNotifyReply) error {
	m.wm.Lock()
	defer m.wm.Unlock()
	//fmt.Printf("Mapped files %d \n", len(args.InterFiles))
	for _, rf := range args.InterFiles {
		rf := rf
		fmt.Printf("File recieved %s \n", rf)
		if m.MappedFileMap[rf] {
			reply.Status = true
			return nil
		}
		m.MappedFiles = append(m.MappedFiles, rf)
		//	fmt.Printf("Current List %v \n", m.MappedFiles)
		m.MappedFileMap[rf] = true
	}
	reply.Status = true
	return nil
}

//NotifyMapCompletion is notify
func (m *Master) NotifyMapCompletion(args *MapWorkerNotify, reply *MapWorkerNotify) error {
	//m.wm.Lock()
	//	defer m.wm.Unlock()
	m.cond.L.Lock()
	defer m.cond.L.Unlock()
	fmt.Printf("Got Notified from worker %d and registered workers are  %d\n", args.Worker, m.MapWorkers)

	if m.MapWorkerMap[args.Worker] {
		var index int
		for i, e := range m.MapWorkers {
			if e == args.Worker {
				index = i
				break
			}
		}
		m.MapWorkers[index] = m.MapWorkers[len(m.MapWorkers)-1]
		m.MapWorkers = m.MapWorkers[:len(m.MapWorkers)-1]
		fmt.Printf("Registered workers  after removing are  %d\n", m.MapWorkers)

	}

	if len(m.MapWorkers) == 0 {
		fmt.Printf("Broadcasting \n")
		// m.cond.L.Lock()
		m.cond.Broadcast()
		//	m.cond.L.Unlock()
	}
	return nil
}

//ReduceNotify is an RPC call made by the worker to request for a task
func (m *Master) ReduceNotify(args *ReduceNotify, reply *ReduceNotifyReply) error {
	m.wm.Lock()
	defer m.wm.Unlock()
	fmt.Println("Got Notified from Reducer", args.File)
	m.ReduceWorkers = append(m.ReduceWorkers, args.File)
	m.ProcessedCount = m.ProcessedCount + 1
	reply.Status = true
	return nil
}

//Notification is used to tell worker
func (m *Master) Notification(args *FinalNotify, reply *NotifyReply) error {
	m.wm.Lock()
	defer m.wm.Unlock()
	if m.ReduceWorkerMap[args.Reducer] {
		var index int
		for i, e := range m.ReduceWorkers {
			if e == args.Reducer {
				index = i
				break
			}
		}
		m.ReduceWorkers[index] = m.ReduceWorkers[len(m.ReduceWorkers)-1]
		m.ReduceWorkers = m.ReduceWorkers[:len(m.ReduceWorkers)-1]
		fmt.Printf("Registered Reduce Workers  after removing are  %s\n", m.ReduceWorkers)
		// m.ReduceWorkers = append(m.ReduceWorkers, w.File)
		// m.ReduceWorkerMap[w.File] = true
		// fmt.Printf("Reducer Files %s\n", m.ReduceWorkers)
	}

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

//Done used main/mrmaster.go calls Done() periodically to find out if the entire job has finished.
func (m *Master) Done() bool {
	ret := false

	m.wm.Lock()
	if len(m.PendingFiles) == 0 && len(m.ReduceWorkers) == 0 && len(m.MappedFiles) == 0 {
		ret = true
	} else {
		fmt.Println("Pending Files Count:", len(m.PendingFiles))
		fmt.Println("ReduceWorkers Files Count:", len(m.ReduceWorkers))
		fmt.Println("MappedFiles Files Count:", len(m.MappedFiles))
		//fmt.Println("Processed Files Count:", m.ProcessedCount)
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
		Files:           files,
		PendingFiles:    files,
		MappedFiles:     make([]string, 0),
		ReduceTCount:    nReduce,
		ProcessedMap:    make(map[string]bool),
		ProcessedCount:  0,
		FailedFiles:     make(map[string]int),
		wm:              sync.Mutex{},
		MappedFileMap:   make(map[string]bool),
		ReduceWorkerMap: make(map[string]bool),
		IsWorker:        false,
		IsReducer:       false,
		MapWorkerMap:    make(map[int]bool),
	}
	m.cond = sync.NewCond(&m.wm)

	for _, f := range m.Files {
		m.ProcessedMap[f] = false
	}

	fmt.Println("Total Number of processed count:", m.ProcessedCount)
	m.server()
	return &m
}

func (m *Master) checkForTaskTimeout(file string, done <-chan interface{}, ticker *time.Ticker) {

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			m.wm.Lock()
			if !m.ProcessedMap[file] {
				m.FailedFiles[file] = m.FailedFiles[file] + 1
				m.PendingFiles = append(m.PendingFiles, file)
				if m.FailedFiles[file] < 4 {
					m.ProcessedMap[file] = false
				} else {
					m.ProcessedMap[file] = true
				}
			}
			m.wm.Unlock()
		}
	}
}

func (m *Master) fetchUnprocessedFile(w int) (fn string, worker int, err error) {
	m.wm.Lock()
	defer m.wm.Unlock()
	if len(m.PendingFiles) > 0 {
		//	fmt.Printf("Started Processing worker %d\n", w)
		fn = m.PendingFiles[0]
		m.PendingFiles = m.PendingFiles[1:]
		if !m.MapWorkerMap[w] {
			m.MapWorkers = append(m.MapWorkers, w)
			m.MapWorkerMap[w] = true
			fmt.Printf("Workers %w \n", m.MapWorkers)
		}
		m.WorkerCount = w
		return fn, m.WorkerCount, nil
	}
	return "", -1, errors.New("Please Exit")

}

func (m *Master) fetchMappedFile(w *ReduceArgs) (fn string, err error) {
	// m.wm.Lock()
	// defer m.wm.Unlock()
	m.cond.L.Lock()
	defer m.cond.L.Unlock()
	for len(m.MapWorkers) > 0 {
		fmt.Printf("Waiting to start %s \n", w.File)
		m.cond.Wait()
	}
	if len(m.MappedFiles) > 0 {
		if !m.ReduceWorkerMap[w.File] {
			m.ReduceWorkers = append(m.ReduceWorkers, w.File)
			m.ReduceWorkerMap[w.File] = true
			fmt.Printf("Reducer Files %s\n", m.ReduceWorkers)
		}
		//	fmt.Printf("Started Processing worker %s\n", w.File)
		fn = m.MappedFiles[0]
		m.MappedFiles = m.MappedFiles[1:]
		return fn, nil
	}
	return "", errors.New("Please Exit")
}
