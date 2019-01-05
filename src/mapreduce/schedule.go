package mapreduce

import (
	"fmt"
)

func handleTask(worker string, args DoTaskArgs, successWorkerChan chan string, failWorkerChan chan string, taskChan chan DoTaskArgs) {
	success := call(worker, "Worker.DoTask", &args, nil)
	if success {
		successWorkerChan <- worker
	} else {
		fmt.Println(worker, "executing task", args.TaskNumber, "has failed")
		taskChan <- args
		failWorkerChan <- worker
	}
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

	taskChan := make(chan DoTaskArgs, ntasks)
	go func() {
		for i := 0; i < ntasks; i++ {
			var args DoTaskArgs
			args.JobName = jobName
			if phase == mapPhase {
				args.File = mapFiles[i]
			}
			args.Phase = phase
			args.TaskNumber = i
			args.NumOtherPhase = n_other
			taskChan <- args
		}
	}()

	successWorkerChan := make(chan string, 11)
	failWorkerChan := make(chan string, 10)
	tasksRemaining := ntasks
	tasksOutstanding := 0
	workerFree := make(map[string]bool)
	// assumption: once a worker dies, don't want to assign any more work to that worker
	for tasksRemaining > 0 {
		// fmt.Println(tasksRemaining, "tasks remaining")
		select {
		case newWorker := <-registerChan:
			workerFree[newWorker] = true
			if tasksRemaining-tasksOutstanding > 0 {
				nextTask := <-taskChan
				tasksOutstanding = tasksOutstanding + 1
				workerFree[newWorker] = false
				go handleTask(newWorker, nextTask, successWorkerChan, failWorkerChan, taskChan)
			}
		case nextWorker := <-successWorkerChan:
			tasksOutstanding = tasksOutstanding - 1
			tasksRemaining = tasksRemaining - 1
			// even if there are tasks remaining, they may have already been scheduled
			if tasksRemaining-tasksOutstanding > 0 {
				nextTask := <-taskChan
				tasksOutstanding = tasksOutstanding + 1
				go handleTask(nextWorker, nextTask, successWorkerChan, failWorkerChan, taskChan)
			} else {
				workerFree[nextWorker] = true
			}
		case failedWorker := <-failWorkerChan:
			delete(workerFree, failedWorker)
			tasksOutstanding = tasksOutstanding - 1
			// if no outstanding tasks, then need to assign to worker; if no outstanding workers, wait for success from existing worker or for new worker
			for worker, isFree := range workerFree {
				if isFree {
					workerFree[worker] = false
					nextTask := <-taskChan
					tasksOutstanding = tasksOutstanding + 1
					go handleTask(worker, nextTask, successWorkerChan, failWorkerChan, taskChan)
					break
				}
			}
		}
	}

	fmt.Printf("Schedule: %v done\n", phase)
}
