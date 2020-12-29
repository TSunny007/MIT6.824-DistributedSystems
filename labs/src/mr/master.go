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

type Stage struct {
	idle       map[string]string
	pending    map[string]Timer
	idleMux    *sync.Mutex
	pendingMux *sync.Mutex
	cond       *sync.Cond
	stageCode  int8
}

type Timer struct {
	s string
	t *time.Timer
}

// Master holds all the information the master thread needs
type Master struct {
	// Your definitions here.
	mStage  Stage
	rStage  Stage
	nReduce int
	doneMux *sync.Mutex
	done    bool
}

// Your code here -- RPC handlers for the worker to call.
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) AssignWork(request *CompletionRPC, reply *AssignRPC) error {
	reply.NReduce = m.nReduce
	if len(request.File) > 0 {
		switch request.Phase {
		case MAP_JOB:
			{
				handleCompletion(request.File, &m.mStage)
			}
		case REDUCE_JOB:
			{
				handleCompletion(request.File, &m.rStage)
			}
		}
	}
	for {
		if assignTask(&m.mStage, reply) {
			reply.Phase = MAP_JOB
			log.Printf("Master - Map: %+v\n", reply)
			return nil
		}
		if shouldSleep(&m.mStage) {
			m.mStage.cond.L.Lock()
			m.mStage.cond.Wait()
			m.mStage.cond.L.Unlock()
			continue
		}
		if phaseComplete(&m.mStage) {
			m.mStage.cond.Broadcast()
			break
		}
	}
	for {
		if assignTask(&m.rStage, reply) {
			reply.Phase = REDUCE_JOB
			log.Printf("Master - Reduce: %+v\n", reply)
			return nil
		}
		if shouldSleep(&m.rStage) {
			m.rStage.cond.L.Lock()
			m.rStage.cond.Wait()
			m.rStage.cond.L.Unlock()
			continue
		}
		if phaseComplete(&m.rStage) {
			m.rStage.cond.Broadcast()
			break
		}
	}
	m.doneMux.Lock()
	m.done = true
	m.doneMux.Unlock()
	reply.Phase = DONE_JOB
	return nil
}

func assignTask(st *Stage, reply *AssignRPC) (r bool) {
	st.idleMux.Lock()
	if len(st.idle) > 0 {
		for file, task := range st.idle {
			reply.Task, reply.File, reply.Phase = task, file, st.stageCode
			delete(st.idle, file)
			r = true
			break
		}
	}
	st.idleMux.Unlock()
	if r {
		st.pendingMux.Lock()
		st.pending[reply.File] = Timer{reply.Task, time.AfterFunc(
			10*time.Second,
			taskTimeout(st, &AssignRPC{reply.File, reply.Task, reply.NReduce, reply.Phase}))}
		st.pendingMux.Unlock()
	}
	return
}

func shouldSleep(st *Stage) bool {
	st.idleMux.Lock()
	defer st.pendingMux.Unlock()
	st.pendingMux.Lock()
	defer st.idleMux.Unlock()
	if len(st.idle) == 0 && len(st.pending) > 0 {
		return true
	}
	return false
}

func phaseComplete(st *Stage) bool {
	st.idleMux.Lock()
	defer st.pendingMux.Unlock()
	st.pendingMux.Lock()
	defer st.idleMux.Unlock()
	return len(st.idle)+len(st.pending) == 0
}

func taskTimeout(st *Stage, reassign *AssignRPC) func() {
	return func() {
		st.idleMux.Lock()
		st.pendingMux.Lock()
		log.Printf("timed out with %s", reassign.File)
		st.idle[reassign.File] = reassign.Task
		delete(st.pending, reassign.File)
		st.idleMux.Unlock()
		st.pendingMux.Unlock()
		st.cond.Broadcast()
	}
}

func handleCompletion(file string, st *Stage) {
	st.pendingMux.Lock()
	Timer, ok := st.pending[file]
	if !ok {
		log.Printf("dead job %s for stage %d", file, st.stageCode)
		return
	}
	Timer.t.Stop()
	delete(st.pending, file)
	if len(st.pending) == 0 {
		st.pendingMux.Unlock()
		st.cond.Broadcast()
	} else {
		st.pendingMux.Unlock()
	}
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

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	m.doneMux.Lock()
	defer m.doneMux.Unlock()
	return m.done
}

// create a Master. main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{nReduce: nReduce, done: false, doneMux: &sync.Mutex{}}
	m.mStage = Stage{
		idle:       make(map[string]string),
		pending:    make(map[string]Timer),
		idleMux:    &sync.Mutex{},
		pendingMux: &sync.Mutex{},
		cond:       &sync.Cond{L: &sync.Mutex{}},
		stageCode:  MAP_JOB}
	for i, file := range files {
		m.mStage.idle[file] = strconv.Itoa(i)
	}

	m.rStage = Stage{
		idle:       make(map[string]string),
		pending:    make(map[string]Timer),
		idleMux:    &sync.Mutex{},
		pendingMux: &sync.Mutex{},
		cond:       &sync.Cond{L: &sync.Mutex{}},
		stageCode:  REDUCE_JOB}
	for n := 0; n < m.nReduce; n++ {
		m.rStage.idle[fmt.Sprintf("mr-*-%d", n)] = strconv.Itoa(n)
	}

	m.server()
	return &m
}
