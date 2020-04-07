package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

//MapArgs is request from worker to master to request for work.
type WorkerArgs struct {
	WokerName int
}

//MapReply is used when Worker asks for Task and master gives back the FilePath of the File.
type WorkerReply struct {
	Files        []string
	ReduceTCount int
	Worker       int
}

//NotifyArgs is used when worker needs to notify master about the Task status
type NotifyArgs struct {
	FilePath string
	Worker   int
}

//NotifyReply is used when master acknowledges that it successfully got a notification about the provided task
type NotifyReply struct {
	Status bool
}

//ReduceArgs is an request from worker to master to request for work.
type ReduceArgs struct{}

//ReduceReply is acknowledgement from worker to master
type ReduceReply struct {
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
