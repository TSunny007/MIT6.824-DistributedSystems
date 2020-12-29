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
// stage number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker runs the map and the reduce stages and communicates
// with the master thread as necessary to get work done
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	completed, assigned := CompletionRPC{}, AssignRPC{}
	for {
		if !call("Master.AssignWork", &completed, &assigned) {
			os.Exit(0)
		}
		log.Printf("Assigned: %+v\n", assigned)
		switch assigned.Phase {
		case MAP_JOB:
			{
				file, err := os.Open(assigned.File)
				if err != nil {
					log.Fatalf("cannot open %s\n", assigned.File)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %s\n", assigned.File)
				}
				file.Close()
				kva := mapf(assigned.File, string(content))
				buckets := make(map[int][]KeyValue)
				for _, kv := range kva {
					bucket := ihash(kv.Key) % assigned.NReduce
					l, ok := buckets[bucket]
					if ok {
						buckets[bucket] = append(l, kv)
					} else {
						buckets[bucket] = []KeyValue{kv}
					}
				}
				for bucket, bucketKva := range buckets {
					sort.Sort(ByKey(bucketKva))
					tmp, err := ioutil.TempFile("", fmt.Sprintf("temp-%s-%d", assigned.Task, bucket))
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
					outputFile := fmt.Sprintf("mr-%s-%d", assigned.Task, bucket)
					// Another job beat us to this.
					if _, err := os.Stat(outputFile); os.IsExist(err) {
						completed, assigned = CompletionRPC{}, AssignRPC{}
						continue
					}
					if err := os.Rename(tmp.Name(), outputFile); err != nil {
						log.Printf("Rename error: %v", err)
					}
				}
			}
		case REDUCE_JOB:
			{
				matches, err := filepath.Glob(assigned.File)
				if err != nil {
					log.Printf("No files matching pattern %s\n", assigned.File)
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

				tmp, err := ioutil.TempFile("", fmt.Sprintf("temp-%s", assigned.Task))
				if err != nil {
					log.Println("tempfile:", err)
					return
				}
				for i := 0; i < len(kva); {
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
				completedFile := fmt.Sprintf("mr-out-%s", assigned.Task)
				// Another job beat us to this.
				if _, err := os.Stat(completedFile); os.IsExist(err) {
					completed, assigned = CompletionRPC{}, AssignRPC{}
					continue
				}
				if err := os.Rename(tmp.Name(), completedFile); err != nil {
					log.Printf("Rename error: %v", err)
				}
			}
		case DONE_JOB:
			{
				os.Exit(0)
			}
		}

		completed.File = assigned.File
		completed.Phase = assigned.Phase
		log.Printf("Completed: %+v\n", completed)
	}
}

//
// the RPC argument and reply types are defined in rpc.go.
//
func RequestWork(completed CompletionRPC) (reply AssignRPC) {
	// declare a reply structure.
	call("Master.AssignWork", &completed, &reply)
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
