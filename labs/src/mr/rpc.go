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

const ( // iota is reset to 0
	mapJob = iota // map -> 0
	reduceJob = iota // reduce -> 1
	doneJob = iota // done -> 2
)

type EmptyArgs struct {
}

type AssignRpc struct {
	Task  string
	File  string
	Phase int
}

type CompletionRpc struct {
	File  * string
	Phase int
}

type NumberRpc struct {
	N int
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
