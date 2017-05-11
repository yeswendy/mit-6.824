package mapreduce

import (
	//"hash/fnv"
	"os"
	"encoding/json"
	"log"
	"sort"
)
// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	var file_kv map[string] []string
	file_kv = make(map[string] []string, 0)

	for i := 0; i < nMap; i++ {
		inter_file_name := reduceName(jobName, i, reduceTaskNumber)
		inter_file, err := os.Open(inter_file_name)
		if err != nil {
			log.Fatal("doReduce: open intermediate file %s, error: %s", inter_file_name, err)
		}
		 dec := json.NewDecoder(inter_file)
		 for {
			 var kv KeyValue
			 err := dec.Decode(&kv)
			 if err != nil {
				break //end of decoding
			 }
			 _, ok := file_kv[kv.Key]
			 if ok == false {
				file_kv[kv.Key] = make([] string,0)
			 } else {
				 file_kv[kv.Key] = append(file_kv[kv.Key], kv.Value)
			 }
		 }
		inter_file.Close()
	}
	var keys []string
	keys = make([]string, 0)
	for k := range file_kv {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	merge_file_name := mergeName(jobName, reduceTaskNumber)
	merge_file, err := os.Create(merge_file_name)
	if err != nil {
		log.Fatal("doReduce: create merge file %s, error: %s", merge_file_name, err)
	}
	//defer merge_file.Close()

	enc := json.NewEncoder(merge_file)
	for _, k := range keys {
		err := enc.Encode(KeyValue{k, reduceF(k, file_kv[k])})
		if err != nil {
			log.Fatal("doReduce: encode merge file error: %s", err)
		}
	}
	merge_file.Close()
}
