package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


//
// use ihash(key) % NReduce to choose the reduce
// Task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker runs the map and the reduce tasks and communicates
// with the master thread as necessary to get work done
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	completedTask := CompletionRpc{}
	for {
		workerTask := RequestWork(&completedTask)
		log.Printf("Assigned File: %s, Task: %s, Phase: %d\n", workerTask.File, workerTask.Task, workerTask.Phase)
		switch workerTask.Phase {
		case mapJob:
			{
				file, err := os.Open(workerTask.File)
				if err != nil {
					log.Fatalf("cannot open %v", workerTask.File)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", workerTask.File)
				}
				file.Close()
				kva := mapf(workerTask.File, string(content))
				buckets := make(map[int][]KeyValue)
				nReduce := RequestNReduce()
				for _, kv := range kva {
					bucket := ihash(kv.Key) % nReduce
					l, ok := buckets[bucket]
					if ok {
						buckets[bucket] = append(l, kv)
					} else {
						buckets[bucket] = []KeyValue{kv}
					}
				}
				for bucket, bucketKva := range buckets {
					sort.Sort(ByKey(bucketKva))
					tmp, err := ioutil.TempFile("", fmt.Sprintf("temp-%s-%d", workerTask.Task, bucket))
					if err != nil {
						log.Println("tempfile:", err)
						return
					}
					enc := json.NewEncoder(tmp)
					for _, kv := range bucketKva {
						err := enc.Encode(&kv)
						if err != nil {
							log.Fatal("Cannot encode a key value", err)
						}
					}

					tmp.Close()
					outputFile := fmt.Sprintf("mr-%s-%d", workerTask.Task, bucket)
					if err := os.Rename(tmp.Name(), outputFile); err != nil {
						log.Printf("Rename error: %v", err)
					}
				}
				completedTask.File = &workerTask.File
			}
		case reduceJob:
			{
				matches, err := filepath.Glob(workerTask.File)
				if err != nil {
					log.Printf("No files matching pattern %s\n", workerTask.File)
				}
				var kva []KeyValue
				for _, file := range matches {
					f, err := os.Open(file)
					if err != nil {
						log.Printf("Couldn't open file %s\n", file)
					}
					dec := json.NewDecoder(f)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						kva = append(kva, kv)
					}
				}
				sort.Sort(ByKey(kva))

				tmp, err := ioutil.TempFile("", fmt.Sprintf("temp-%s", workerTask.Task))
				if err != nil {
					log.Println("tempfile:", err)
					return
				}
				for i:= 0; i < len(kva); {
					j := i + 1
					for j < len(kva) && kva[j].Key == kva[i].Key {
						j++
					}
					var values []string
					for k := i; k < j; k++ {
						values = append(values, kva[k].Value)
					}
					output := reducef(kva[i].Key, values)

					// this is the correct format for each line of Reduce output.
					_, err := fmt.Fprintf(tmp, "%v %v\n", kva[i].Key, output)
					if err != nil {
						log.Fatalf("problem with writing %v %v to %s\n", kva[i].Key, output, tmp.Name())
					}
					i = j
				}
				completedFile := fmt.Sprintf("mr-out-%s", workerTask.Task)
				err = os.Rename(tmp.Name(), completedFile)
				if err != nil {
					log.Printf("Rename error: %v", err)
				}
				completedTask.File = &workerTask.File
			}
		case doneJob:
			{
				os.Exit(0)
			}
		}
		completedTask.Phase = workerTask.Phase
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func RequestWork(completedTask * CompletionRpc) (reply AssignRpc) {
	// declare a reply structure.
	if completedTask != nil {
		call("Master.RespondWork", completedTask, &reply)
	} else {
		call("Master.RespondWork", &CompletionRpc{}, &reply)
	}
	return
}

func RequestNReduce() (reply int) {
	call("Master.RespondNReduce", &EmptyArgs{}, &reply)
	return
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
