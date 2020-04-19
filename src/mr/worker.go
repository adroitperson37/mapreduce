package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

//KeyValue is used to split
type KeyValue struct {
	Key   string
	Value string
}

//ByKey for sorting by key.
type ByKey []KeyValue

//WorkerWrapper is used as a wrapper
type WorkerWrapper struct {
	Rlen int
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

//Worker is used to do map tasks
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	var wg sync.WaitGroup
	done := make(chan bool)
	defer close(done)
	wg.Add(1)
	rand.Seed(time.Now().UnixNano())
	work := rand.Intn(1000)

	wStream := callMapChan(done, work, mapf)
	fileStream := splitFilesOneChan(done, wStream)
	CallMapChanNotify(done, &wg, fileStream)
	wg.Wait()
	NotifyMaster(work)
	fmt.Println("Exiting")
	var wg1 sync.WaitGroup
	rand.Seed(time.Now().UnixNano())
	red := rand.Intn(1000)
	fmt.Printf("Reducer filename %d \n", red)
	oname := fmt.Sprintf("mr-out-%d.txt", red)

	wg1.Add(1)
	reduceStream := callReduceChan(done, oname, reducef)

	for data := range reduceStream {
		temp := data.Data

		sort.Sort(ByKey(temp))
		actualFileName := fmt.Sprintf("mr-out-%d.txt", data.File)
		tempFile := fmt.Sprintf("mr-tmp-%s-%d", randomString(6), data.File)
		ofile, _ := os.Create(tempFile)
		i := 0
		for i < len(temp) {
			j := i + 1
			for j < len(temp) && temp[j].Key == temp[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, temp[k].Value)
			}
			output := reducef(temp[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", temp[i].Key, output)

			i = j

		}
		ofile.Close()
		if err := os.Rename(tempFile, actualFileName); err != nil {
			log.Fatalf("cannot rename %s to %s", tempFile, actualFileName)
		}
		os.Remove(tempFile)
	}

	callFinalNotify(&wg1, oname)
	wg1.Wait()
}

var defaultLetters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

// randomString returns a random string with a fixed length
func randomString(n int, allowedChars ...[]rune) string {
	var letters []rune

	if len(allowedChars) == 0 {
		letters = defaultLetters
	} else {
		letters = allowedChars[0]
	}

	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}

	return string(b)
}

func createReduceFile(done chan bool, stream chan WorkerWrapper, reducef func(string, []string) string) chan bool {
	finStream := make(chan bool)
	intermediate := make([]KeyValue, 0)
	go func(reducef func(string, []string) string) {
		defer close(finStream)
		for {
			select {
			case d, ok := <-stream:
				if !ok {
					return
				}
				intermediate = append(intermediate, d.Data...)

				sort.Sort(ByKey(intermediate))
				i := 0
				for i < len(intermediate) {

					tempFile := getReduceFileName(10, intermediate[i].Key)
					file, err := os.OpenFile(tempFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
					if err != nil {
						log.Fatal(err)
					}

					j := i + 1
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					output := reducef(intermediate[i].Key, values)

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)

					i = j
					file.Close()

				}
			}
		}

	}(reducef)
	return finStream
}

func splitFilesOneChan(done chan bool, stream chan WorkerWrapper) chan []string {
	fileStream := make(chan []string)
	dm := make(map[string]bool)
	go func() {
		defer close(fileStream)
		for {
			select {
			case <-done:
				return
			case intermediate, ok := <-stream:
				if !ok {
					return
				}
				interFiles := make([]string, 0)
				for _, kv := range intermediate.Data {
					//fmt.Println("REcieved Data from :", intermediate.File)
					tempFile := getTempFileName(10, kv.Key)
					//fmt.Printf("Created map file %s \n", tempFile)
					file, err := os.OpenFile(tempFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
					if err != nil {
						log.Fatal(err)
					}
					enc := json.NewEncoder(file)
					err = enc.Encode(&kv)
					if err != nil {
						fmt.Println(err)
					}
					if !dm[tempFile] {
						interFiles = append(interFiles, tempFile)
						dm[tempFile] = true
					}

					file.Close()

				}
				fileStream <- interFiles
			}
		}
	}()

	return fileStream
	// var wg sync.WaitGroup
	// fileStream := make(chan []string)
	// fmt.Println("Channel Length", len(stream))
	// multiplex := func(dStream chan WorkerWrapper) {
	// 	defer wg.Done()
	// 	// for data := range dStream {
	// 	// 	fmt.Println("Gere at 5")
	// 	// 	fmt.Printf("Data %v \n", data)
	// 	// }
	// 	for {
	// 		select {
	// 		case <-done:
	// 			return
	// 		case d, ok := <-dStream:
	// 			if !ok {
	// 				fmt.Println("Closing")
	// 				return
	// 			}
	// 			interFiles := make([]string, 0)
	// 			for _, kv := range d.Data {
	// 				//	fmt.Println("REcieved Data from :", intermediate.File)
	// 				tempFile := getTempFileName(10, d.File, kv.Key)
	// 				file, err := os.OpenFile(tempFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
	// 				if err != nil {
	// 					log.Fatal(err)
	// 				}
	// 				enc := json.NewEncoder(file)
	// 				err = enc.Encode(&kv)
	// 				if err != nil {
	// 					fmt.Println(err)
	// 				}
	// 				interFiles = append(interFiles, tempFile)
	// 				file.Close()

	// 			}
	// 			fileStream <- interFiles
	// 		}

	// 	}

	// }

	// wg.Add(len(stream))

	// for _, c := range stream {
	// 	go multiplex(c)
	// }

	// go func() {
	// 	wg.Wait()
	// 	close(fileStream)
	// 	fmt.Println("Closed File Stream from split files")
	// }()

	// return fileStream

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

func callReduce(rf string, reducef func(string, []string) string) (bool, []KeyValue) {

	files, status := CallReduceRPC(rf)
	if !status {
		return status, nil
	}
	intermediate := make([]KeyValue, 0)
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

	return true, intermediate

}

func callMapChan(done chan bool, work int, mapf func(string, string) []KeyValue) chan WorkerWrapper {

	wStream := make(chan WorkerWrapper)

	go func() {
		defer close(wStream)
	loop:
		for {
			//	st := time.Now()
			intermediate := []KeyValue{}
			responseFiles, rc, wn := CallMapRPC(work)
			fmt.Println("Current Worker:", wn)
			if wn == -1 {
				fmt.Println("No Workers hence exiting. Current worker:", wn)
				break loop
			}
			for _, filename := range responseFiles {
				fmt.Printf("Recieved File %s \n", filename)
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
				Rlen: rc,
				File: 0,
				Data: intermediate,
			}
			wStream <- ww
			// time.Sleep(1)
		}
	}()
	return wStream
}

func callReduceChan(done chan bool, fname string, reducef func(string, []string) string) chan WorkerWrapper {
	wStream := make(chan WorkerWrapper)

	go func(reducef func(string, []string) string, fname string) {
		defer close(wStream)
	loop:
		for {
			//	st := time.Now()
			intermediate := []KeyValue{}
			//fmt.Printf("Requesting File Name %s \n", fname)
			responseFiles, status := CallReduceRPC(fname)
			time.Sleep(1 * time.Second)

			if !status {
				fmt.Println("No Reduce workers hence exiting.")
				break loop
			}
			var rn int
			for _, filename := range responseFiles {
				res1 := strings.Split(filename, "-")
				num := strings.Split(res1[2], ".")
				// Displaying the result
				rn, _ = strconv.Atoi(num[0])

				fmt.Printf("Recieved %s \n", filename)
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

			wc := WorkerWrapper{Data: intermediate, File: rn}
			wStream <- wc

			// sort.Sort(ByKey(intermediate))
			// i := 0
			// for i < len(intermediate) {

			// 	tempFile := getReduceFileName(10, intermediate[i].Key)
			// 	// if fileExists(tempFile) {
			// 	// 	existingData := []KeyValue{}
			// 	// 	f, err := os.OpenFile(tempFile, os.O_RDONLY, os.ModePerm)
			// 	// 	if err != nil {
			// 	// 		log.Fatalf("open file error: %v", err)
			// 	// 		return
			// 	// 	}
			// 	// 	sc := bufio.NewScanner(f)
			// 	// 	for sc.Scan() {
			// 	// 		data := sc.Text() // GET the line string
			// 	// 		values := strings.Split(data, " ")
			// 	// 		td := KeyValue{Key: values[0], Value: values[1]}
			// 	// 		existingData = append(existingData, td)

			// 	// 	}
			// 	// 	if err := sc.Err(); err != nil {
			// 	// 		log.Fatalf("scan file error: %v", err)
			// 	// 		return
			// 	// 	}
			// 	// 	f.Close()

			// 	// 	file, err := os.OpenFile(tempFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
			// 	// 	if err != nil {
			// 	// 		log.Fatal(err)
			// 	// 	}
			// 	// 	j := i + 1
			// 	// 	for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			// 	// 		j++
			// 	// 	}
			// 	// 	values := []string{}
			// 	// 	for k := i; k < j; k++ {
			// 	// 		values = append(values, intermediate[k].Value)
			// 	// 	}
			// 	// 	output := reducef(intermediate[i].Key, values)

			// 	// 	for _, v := range existingData {
			// 	// 		if v.Key == intermediate[i].Key {
			// 	// 			if v.Key == "A" {
			// 	// 				fmt.Println("Before ", output)
			// 	// 			}

			// 	// 			b, _ := strconv.Atoi(output)
			// 	// 			c, _ := strconv.Atoi(v.Value)
			// 	// 			result := b + c
			// 	// 			if v.Key == "A" {
			// 	// 				fmt.Println("After ", strconv.Itoa(result))
			// 	// 			}

			// 	// 			output = strconv.Itoa(result)
			// 	// 		}
			// 	// 	}
			// 	// 	fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)
			// 	// 	i = j
			// 	// 	file.Close()

			// 	// } else {

			// 	fmt.Printf("Reducer file %s \n", tempFile)
			// 	file, err := ioutil.TempFile(".", tempFile)
			// 	file, err := os.OpenFile(tempFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
			// 	if err != nil {
			// 		log.Fatal(err)
			// 	}

			// 	j := i + 1
			// 	for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			// 		j++
			// 	}
			// 	values := []string{}
			// 	for k := i; k < j; k++ {
			// 		values = append(values, intermediate[k].Value)
			// 	}
			// 	output := reducef(intermediate[i].Key, values)

			// 	// this is the correct format for each line of Reduce output.
			// 	fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)
			// 	i = j
			// 	file.Close()

			// 	//	}

			// }

			//	wStream <- true
			// time.Sleep(1)
		}
	}(reducef, fname)
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

func getTempFileName(rc int, f string) string {
	rt := ihash(f) % rc
	fn := fmt.Sprintf("mr-%d-%d.txt", rt, rt)
	return fn
}

func getReduceFileName(rc int, f string) string {
	rt := ihash(f) % rc
	fn := fmt.Sprintf("mr-out-%d.txt", rt)
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

//CallNotify is used to notify master about the completion of reducer
func CallNotify(file string) bool {
	args := NotifyArgs{Reducer: file}

	reply := NotifyReply{}

	call("Master.WorkerNotify", &args, &reply)
	return reply.Status
}

func callFinalNotify(wg *sync.WaitGroup, fname string) {
	args := FinalNotify{Reducer: fname}
	reply := FinalNotifyReply{}
	call("Master.Notification", &args, &reply)
	wg.Done()
}

//CallMapNotify is used to notify master about intermittent files
func CallMapNotify(files []string) bool {
	args := MapNotifyArgs{InterFiles: files}
	reply := MapNotifyReply{}
	call("Master.MapNotify", &args, &reply)
	return reply.Status
}

func callReduceNotify(file string) bool {

	fmt.Println("Sending Worker to register at Master:", file)
	args := ReduceNotify{File: file}
	reply := ReduceNotifyReply{}
	call("Master.ReduceNotify", &args, &reply)
	return true
}

//CallMapChanNotify is used to notify the master
func CallMapChanNotify(done chan bool, wg *sync.WaitGroup, stream chan []string) {

	closeChannel := func(wg *sync.WaitGroup) {
		wg.Done()
		//fmt.Println("Closed")
	}
	go func() {
		defer closeChannel(wg)
		for {
			//	fmt.Println("Here 6")
			select {
			case <-done:
				return
			case files, ok := <-stream:
				if !ok {
					return
				}
				args := MapNotifyArgs{InterFiles: files}
				reply := MapNotifyReply{}
				call("Master.MapNotify", &args, &reply)
			}
		}
	}()

}

//CallReduceRPC is used call Reduce
func CallReduceRPC(file string) ([]string, bool) {
	args := ReduceArgs{File: file}
	reply := ReduceReply{}
	call("Master.ReduceReq", &args, &reply)
	//fmt.Printf("reply.Y %v\n", reply.Files)
	return reply.Files, reply.Status

}

//
//NotifyMaster is used to notify about worker completion
func NotifyMaster(work int) {
	args := MapWorkerNotify{Worker: work}
	reply := MapWorkerReply{}
	// send the RPC request, wait for the reply.
	call("Master.NotifyMapCompletion", &args, &reply)
}

//CallMapRPC is used to request task from master
func CallMapRPC(work int) ([]string, int, int) {
	// declare an argument structure.
	// rand.Seed(time.Now().UnixNano())
	// wn := rand.Intn(100)
	args := WorkerArgs{WokerName: work}

	// declare a reply structure.
	reply := WorkerReply{}

	// send the RPC request, wait for the reply.
	call("Master.WorkerReq", &args, &reply)

	// reply.Y should be 100.
	//	fmt.Printf("reply.Y %v\n", reply.Files)
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
