package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)


type MRStage struct {
	v map[string] string
	mux sync.Mutex
}

type MRTimer struct {
	s string
	t *time.Timer
}

type MRPendingStage struct {
	v map[string] MRTimer
	mux sync.Mutex
}

// Master holds all the information the master thread needs
type Master struct {
	// Your definitions here.
	// mapping of filename to Task number
	mapIdle       MRStage // initially starts out with all of our files
	mapPending    MRPendingStage
	reduceIdle    MRStage
	reducePending MRPendingStage
	nReduce       int
	mapCond       * sync.Cond
	reduceCond	  * sync.Cond
}

// Your code here -- RPC handlers for the worker to call.

// RequestWork is called by workers
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) RespondWork(request *CompletionRpc, reply *AssignRpc) error {
	if request.File != nil {
		switch request.Phase {
		case mapJob:
			{
				handleCompletion(*request.File, &m.mapPending, m.mapCond)
			}
		case reduceJob:
			{
				handleCompletion(*request.File, &m.reducePending, m.reduceCond)
			}
		}
		request.File = nil // we have handled this completion request
	}
	/**************** Assign map Task ******************/
	if assignTask(&m.mapIdle, &m.mapPending, reply, mapJob, m.mapCond) {
		return nil
	}
	/********* Cond wait for map tasks to finish *******/
	if sleepForTask(&m.mapIdle, &m.mapPending, m.mapCond) {
		return m.RespondWork(request, reply)
	}
	/**************** Assign reduce Task  ****************/
	if assignTask(&m.reduceIdle, &m.reducePending, reply, reduceJob, m.reduceCond) {
		return nil
	}
	/*********** Wait for reduce tasks to finish  *********/
	if sleepForTask(&m.reduceIdle, &m.reducePending, m.reduceCond) {
		return m.RespondWork(request, reply)
	}

	reply.Phase = doneJob
	return nil
}

func assignTask(idle *MRStage, pending *MRPendingStage, reply *AssignRpc, job int, cond *sync.Cond) bool {
	idle.mux.Lock(); defer idle.mux.Unlock()
	if len(idle.v) > 0 {
		pending.mux.Lock(); defer pending.mux.Unlock()
		for file, task := range idle.v {
			reply.File, reply.Task, reply.Phase = file, task, job
			delete(idle.v, file)
			pending.v[file] = MRTimer{task, time.AfterFunc(
				10*time.Second,
				taskTimeout(idle, pending, AssignRpc{task, file, job}, cond),
			)}
			return true
		}
	}
	return false
}

func taskTimeout(idle *MRStage, pending *MRPendingStage, reassign AssignRpc, cond *sync.Cond) func() {
	return func() {
		idle.mux.Lock(); defer idle.mux.Unlock()
		pending.mux.Lock(); defer pending.mux.Unlock()
		defer cond.Broadcast()
		idle.v[reassign.File] = reassign.Task
		delete(pending.v, reassign.File)
	}
}

func sleepForTask(idle *MRStage, pending *MRPendingStage, cond * sync.Cond) bool {
	// pending.mux == cond.L, so wait gives up the lock
	if &pending.mux != cond.L {
		log.Fatalf("unsynchronized mutex and conditional variable, deadlock will happen\n")
	}
	// this segment is weirdly coded to make sure there's no deadlock with pending and idle
	for ; ; {
		pending.mux.Lock(); tmp1 := len(pending.v)
		if tmp1 > 0 {
			cond.Wait()
			tmp1 = len(pending.v) // condition could have changed
		}
		pending.mux.Unlock()
		idle.mux.Lock(); tmp2 := len(idle.v); idle.mux.Unlock()
		if  tmp2 > 0 {
			return true // do we need to go back and redo a job?
		}
		if tmp1 + tmp1 == 0 { // both empty
			break
		}
	}
	return false
}

func handleCompletion(file string, pending *MRPendingStage, cond *sync.Cond) {
	pending.mux.Lock(); defer pending.mux.Unlock()
	mrTimer, ok := pending.v[file]
	if !ok {
		log.Printf("couldn't find %s in map pending, could be a dead job", file)
		return
	}
	mrTimer.t.Stop()
	delete(pending.v, file)
	if len(pending.v) == 0 {
		cond.Broadcast()
	}
}

// NReduce tells the workers how many buckets to split
// the intermediate map outputs into
func (m *Master) RespondNReduce(_ *EmptyArgs, reply *int) error {
	*reply = m.nReduce
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
func (m *Master) Done() (r bool) {
	m.mapIdle.mux.Lock(); defer m.mapIdle.mux.Unlock()
	m.mapPending.mux.Lock(); defer m.mapPending.mux.Unlock()
	m.reduceIdle.mux.Lock(); defer m.reduceIdle.mux.Unlock()
	m.reducePending.mux.Lock(); defer m.reducePending.mux.Unlock()
	r = (len(m.mapIdle.v) + len(m.mapPending.v) +
		len(m.reduceIdle.v) + len(m.reducePending.v)) == 0
	return
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.mapIdle = MRStage{v: make(map[string] string)}
	for i, file := range files {
		m.mapIdle.v[file] = strconv.Itoa(i)
	}
	m.mapPending = MRPendingStage{v: make(map[string]MRTimer)}
	m.reduceIdle = MRStage{v: make(map[string] string)}
	for n := 0; n < nReduce; n++ {
		m.reduceIdle.v[fmt.Sprintf("mr-*-%d", n)] = strconv.Itoa(n)
	}
	m.reducePending = MRPendingStage{v: make(map[string]MRTimer)}
	m.nReduce = nReduce
	m.mapCond = sync.NewCond(&m.mapPending.mux)
	m.reduceCond = sync.NewCond(&m.reducePending.mux)

	m.server()
	return &m
}
