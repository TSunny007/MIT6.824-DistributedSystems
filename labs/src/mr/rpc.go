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
	MAP_JOB    = iota // map -> 0
	REDUCE_JOB = iota // reduce -> 1
	DONE_JOB   = iota // done -> 2
)

// Add your RPC definitions here.

type AssignRPC struct {
	File    string // the file used for map/ reduce tasks. Reduce tasks have wildcards
	Task    string // task num identifier
	NReduce int    // number of reduce buckets. This never changes.
	Phase   int8   // Map or Reduce
}

type CompletionRPC struct {
	File  string // what file we just finshed: same as the file used for the AssignRPC
	Phase int8   // Map or Reduce
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
