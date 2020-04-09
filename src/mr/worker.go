package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}


// for sorting by key.
type ByKey []KeyValue

type WorkerWrapper struct {
	File int
	Data []KeyValue
}

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

	//fileStream := splitFiles(mapStream)
	var wg sync.WaitGroup
	done:= make(chan bool)
	defer close(done)
	wg.Add(1)
	wStream := callMapChan(wg,done,mapf)
	fileStream := splitFiles(done,wStream)
	CallMapChanNotify(done,fileStream)
	wg.Wait()


	//callMs := true
	//callRed :=true
	//tempFiles := make([]string,0)
	//for callMs {
	//	mres := callMaster(mapf,&tempFiles)
	//	callMs = mres
	//	time.Sleep(1 * time.Second)
	//}
	//tempFiles = unique(tempFiles)
	//CallMapNotify(tempFiles)
	//CallMapChanNotify(fileStream)



	///Reduce Start
	//rand.Seed(time.Now().UnixNano())
	//red := rand.Intn(1000)
	//fmt.Printf("Reducer filename %d \n", red)
	//oname := fmt.Sprintf("mr-out-%d.txt", red)
	//ofile, _ := os.Create(oname)
	//
	//intermediate := make([]KeyValue,0)
	//for callRed{
	//	res,ikv := callReduce(reducef)
	//	callRed = res
	//	intermediate = append(intermediate,ikv...)
	//	time.Sleep(1 * time.Second)
	//}
	//sort.Sort(ByKey(intermediate))
	//i := 0
	//for i < len(intermediate) {
	//	j := i + 1
	//	for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
	//		j++
	//	}
	//	values := []string{}
	//	for k := i; k < j; k++ {
	//		values = append(values, intermediate[k].Value)
	//	}
	//	output := reducef(intermediate[i].Key, values)
	//
	//	// this is the correct format for each line of Reduce output.
	//	fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
	//
	//	i = j
	//}
	//ofile.Close()
	//CallNotify("wc", 0)
	///Reduce End
	//
	//ofile, _ := os.Create(oname)
	//intermediate1 := []KeyValue{}
	//var fm sync.Mutex
	//fm.Lock()
	//for _, tf := range tfl {
	//	file, err := os.Open(tf)
	//	if err != nil {
	//		log.Fatalf("cannot open %v", tf)
	//	}
	//	dec := json.NewDecoder(file)
	//	for {
	//		var kv KeyValue
	//		if err := dec.Decode(&kv); err != nil {
	//			break
	//		}
	//		intermediate1 = append(intermediate1, kv)
	//	}
	//}
	//sort.Sort(ByKey(intermediate1))
	//
	//fm.Unlock()
	//i := 0
	//for i < len(intermediate1) {
	//	j := i + 1
	//	for j < len(intermediate1) && intermediate1[j].Key == intermediate1[i].Key {
	//		j++
	//	}
	//	values := []string{}
	//	for k := i; k < j; k++ {
	//		values = append(values, intermediate1[k].Value)
	//	}
	//	output := reducef(intermediate1[i].Key, values)
	//
	//	// this is the correct format for each line of Reduce output.
	//	fmt.Fprintf(ofile, "%v %v\n", intermediate1[i].Key, output)
	//
	//	i = j
	//}
	//for _, f := range tempFiles {
	//	os.Remove(f)
	//}

}



func splitFiles(done chan bool, stream <-chan WorkerWrapper) chan []string {
	fileStream :=make(chan []string)

	go func() {
		defer close(fileStream)
		for{
			select {
			case <-done:
				return
			case intermediate,ok := <-stream:
				if !ok{
					return
				}
				interFiles := make([]string,0)
				for _, kv := range intermediate.Data    {

					tempFile := getTempFileName(10, intermediate.File, kv.Key)
					file, err := os.OpenFile(tempFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
					if err != nil {
						log.Fatal(err)
					}
					enc := json.NewEncoder(file)
					err = enc.Encode(&kv)
					if err != nil {
						fmt.Println(err)
					}
					interFiles = append(interFiles,tempFile)
					file.Close()

				}
				fileStream <- interFiles
			}
		}
	}()

	return fileStream

}

func unique(stringSlice []string) []string {
	keys := make(map[string]bool)
	list := []string{}
	for _, entry := range stringSlice {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}

func callReduce(reducef func(string, []string) string) (bool,[]KeyValue) {

	 files,status := CallReduceRPC()
	 if !status {
		 return status,nil
	 }
	intermediate := make([]KeyValue,0)
	 for _, filename := range files {
		 file, err := os.Open(filename)
		 if err != nil {
			 log.Fatalf("cannot open %v", filename)
		 }
		 dec := json.NewDecoder(file)
		 for {
			 var kv KeyValue
			 if err := dec.Decode(&kv); err != nil {
				 break
			 }
			 intermediate = append(intermediate, kv)
		 }
	 }

	return true,intermediate

}

//TODO:Seperate Map and Reduce here
func callMaster(mapf func(string, string) []KeyValue, tempFiles *[]string) bool {

	intermediate := []KeyValue{}
	responseFiles, rc, wn := CallMapRPC()
	if wn == -1 {
		return false
	}
	//interFiles := make([]string,0)

	for _, filename := range responseFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	for _, kv := range intermediate {

		tempFile := getTempFileName(rc, wn, kv.Key)

		if containsFile(*tempFiles, tempFile) {
			file, err := os.OpenFile(tempFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
			if err != nil {
				log.Fatal(err)
			}
			enc := json.NewEncoder(file)
			err = enc.Encode(&kv)
			if err != nil {
				fmt.Println(err)
			}
			file.Close()

		} else {
			*tempFiles = append(*tempFiles, tempFile)
			file, err := os.Create(tempFile)
			if err != nil {
				log.Fatal(err)
			}
			enc := json.NewEncoder(file)
			err = enc.Encode(&kv)
			if err != nil {
				fmt.Println(err)
			}
			file.Close()
		}

	}
	return true
	//return CallMapNotify(tempFiles),wn

	//	sort.Sort(ByKey(*intermediate))

	//for _,kv := range intermediate{
	//	tempFile := createTempFile(rTasks,wc,kv)
	//	interFiles = append(interFiles,tempFile.Name())
	//	enc := json.NewEncoder(tempFile)
	//	err := enc.Encode(&kv)
	//	tempFile.Close()
	//	if err != nil {
	//		//Notify Master
	//		status := CallNotify(kv.Key,wc)
	//		fmt.Printf("Map completed for File %v \n",status)
	//	}
	//}

	//for _,kv := range intermediate{
	//	tempFile := createTempFile(rTasks,wc,kv)
	//	interFiles = append(interFiles,tempFile.Name())
	//	enc := json.NewEncoder(tempFile)
	//	err := enc.Encode(&kv)
	//	tempFile.Close()
	//	if err != nil {
	//		//Notify Master
	//		status := CallNotify(kv.Key,wc)
	//		fmt.Printf("Map completed for File %v \n",status)
	//	}
	//}

	//intermediate1 := []KeyValue{}
	//for _,temp := range interFiles{
	//	file, err := os.Open(temp)
	//	if err != nil {
	//		log.Fatalf("cannot open %v", temp)
	//	}
	//	dec := json.NewDecoder(file)
	//	for {
	//		var kv KeyValue
	//		if err := dec.Decode(&kv); err != nil {
	//			break
	//		}
	//		intermediate1 = append(intermediate1, kv)
	//	}
	//}

	//TODO: Seperate this into another task

	//oname := fmt.Sprintf("mr-out-%d.txt",wc)
	//
	//ofile, _ := os.Create(oname)
	//
	//i := 0
	//for i < len(intermediate) {
	//	j := i + 1
	//	for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
	//		j++
	//	}
	//	values := []string{}
	//	for k := i; k < j; k++ {
	//		values = append(values, intermediate[k].Value)
	//	}
	//	output := reducef(intermediate[i].Key, values)
	//
	//	// this is the correct format for each line of Reduce output.
	//	fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
	//
	//	i = j
	//}
	//
	//for _, f := range interFiles{
	//	os.Remove(f)
	//}
	//ofile.Close()
	//CallNotify("wc",0)
	//return true, wn
}


func callMapChan(wg sync.WaitGroup, done chan bool, mapf func(string, string) []KeyValue, ) (chan WorkerWrapper) {

	wStream :=make(chan WorkerWrapper)

	go func() {
		defer close(wStream)
		loop:
			for{
				intermediate := []KeyValue{}
				responseFiles, _, wn := CallMapRPC()
				fmt.Println("message received for:",wn)
				if wn == -1 {
					done <- true
					wg.Done()
					break loop
				}
				for _, filename := range responseFiles {
					file, err := os.Open(filename)
					if err != nil {
						log.Fatalf("cannot open %v", filename)
					}
					content, err := ioutil.ReadAll(file)
					if err != nil {
						log.Fatalf("cannot read %v", filename)
					}
					file.Close()
					kva := mapf(filename, string(content))
					intermediate = append(intermediate, kva...)
				}

				ww := WorkerWrapper{
					File: wn,
					Data: intermediate,
				}

				wStream <- ww
			}
	}()
	return wStream
}


func containsFile(s []string, file string) bool {

	for _, f := range s {
		if file == f {
			return true
		}
	}
	return false
}

//func createTempFile(rc int, wc int, f KeyValue) *os.File {
//	rt := ihash(f.Key) % rc
//	fn := fmt.Sprintf("mr-%d-%d.txt",wc,rt)
//	file, err := ioutil.TempFile(".", fn)
//	if err != nil {
//		log.Fatal(err)
//	}
//	return file
//}

func getTempFileName(rc int, wc int, f string) string {
	rt := ihash(f) % rc
	fn := fmt.Sprintf("mr-%d-%d.txt", wc, rt)
	return fn
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

func CallNotify(file string, w int) bool {
	args := NotifyArgs{FilePath: file, Worker: w}

	reply := NotifyReply{}

	call("Master.WorkerNotify", &args, &reply)
	return reply.Status
}

func CallMapNotify(files []string) bool {
	args := MapNotifyArgs{InterFiles:files}
	reply := MapNotifyReply{}
	call("Master.MapNotify",&args,&reply)
	return reply.Status
}

func CallMapChanNotify(done chan bool, stream chan []string) {
	go func() {
		for{
			select {
			case <-done:
				return
				case files,ok := <-stream:
					if !ok{
						return
					}
					args := MapNotifyArgs{InterFiles:files}
					reply := MapNotifyReply{}
					call("Master.MapNotify",&args,&reply)
			}
		}
	}()

}


func CallReduceRPC() ([]string,bool)  {
	args := ReduceArgs{}
	reply := ReduceReply{}
	call("Master.ReduceReq",&args,&reply)
	fmt.Printf("reply.Y %v\n", reply.Files)
	return reply.Files,reply.Status

}

func CallMapRPC() ([]string, int, int) {
	// declare an argument structure.
	wn := rand.Intn(100)
	args := WorkerArgs{WokerName: wn}

	// declare a reply structure.
	reply := WorkerReply{}

	// send the RPC request, wait for the reply.
	call("Master.WorkerReq", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Files)
	if len(reply.Files) == 0 {
		reply.Worker = -1
	}
	return reply.Files, reply.ReduceTCount, reply.Worker
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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
