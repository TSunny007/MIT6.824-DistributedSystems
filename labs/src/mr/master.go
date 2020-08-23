package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

// Master holds all the information the master thread needs
type Master struct {
	// Your definitions here.
	remaining []string
	ongoing   []string
	finished  []string
	nReduce   int
}

// Your code here -- RPC handlers for the worker to call.

// RequestWork is called by workers
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) RequestWork(_ *EmptyArgs, reply *RequestWorkReply) error {
	if len(m.remaining) > 0 {
		reply.R = m.remaining[0]
		reply.done = false
		m.ongoing = append(m.ongoing, reply.R)
		m.remaining = m.remaining[1:]
	} else {
		reply.R = ""
		reply.done = true
	}
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

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.remaining = files
	m.nReduce = nReduce
	m.server()
	return &m
}
