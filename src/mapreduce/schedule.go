package mapreduce

import (
	"fmt"
	"sync"
)

func handleTask(worker string, args DoTaskArgs, finishChan chan string, wg *sync.WaitGroup) {
	defer wg.Done()

	call(worker, "Worker.DoTask", &args, nil)
	finishChan <- worker
}

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.

	// scheduling policy: every worker's goroutine has a channel; when a worker can be assigned work, a new goroutine is created to schedule the work, wait for a response, and then write to the worker's channel that it is done
	// select on:
	//    registerChan (new worker):
	//        add worker to workers list
	//        assign work to worker
	//    workerChannels (each worker):
	finishChan := make(chan string, 10)
	var wg sync.WaitGroup
	for i := 0; i < ntasks; i++ {
		var args DoTaskArgs
		args.JobName = jobName
		if phase == mapPhase {
			args.File = mapFiles[i]
		}
		args.Phase = phase
		args.TaskNumber = i
		args.NumOtherPhase = n_other

		select {
		case newWorker := <-registerChan:
			wg.Add(1)
			// fmt.Println("found new worker", newWorker)
			go handleTask(newWorker, args, finishChan, &wg)
		case nextWorker := <-finishChan:
			wg.Add(1)
			// fmt.Println(nextWorker, "reported done")
			go handleTask(nextWorker, args, finishChan, &wg)
		}
	}

	wg.Wait()

	fmt.Printf("Schedule: %v done\n", phase)
}
