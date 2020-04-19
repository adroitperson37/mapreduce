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

//WorkerArgs is request from worker to master to request for work.
type WorkerArgs struct {
	WokerName int
}

//WorkerReply is used when Worker asks for Task and master gives back the FilePath of the File.
type WorkerReply struct {
	Files        []string
	ReduceTCount int
	Worker       int
}

//NotifyArgs is used when worker needs to notify master about the Task status
type NotifyArgs struct {
	Reducer string
}

//MapNotifyArgs used
type MapNotifyArgs struct {
	InterFiles []string
}

//MapNotifyReply used
type MapNotifyReply struct {
	Status bool
}

//MapWorkerNotify is used
type MapWorkerNotify struct {
	Worker int
}

type MapWorkerReply struct {
}

//NotifyReply is used when master acknowledges that it successfully got a notification about the provided task
type NotifyReply struct {
	Status bool
}

//ReduceArgs is an request from worker to master to request for work.
type ReduceArgs struct {
	File string
}

//ReduceReply is acknowledgement from worker to master
type ReduceReply struct {
	Files  []string
	Status bool
}

//ReduceNotify is used to notify master about the reducer task started
type ReduceNotify struct {
	File string
}

//FinalNotify is to tell master to exit
type FinalNotify struct {
	Reducer string
}

//FinalNotifyReply is to tell master
type FinalNotifyReply struct {
}

type ReduceNotifyReply struct {
	Status bool
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
