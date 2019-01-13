### Timers

1. Election timeout (to start election)
2. Election win timeout (to declare election lost)
3. Heartbeat timeout (only for leaders)

### Goroutines

Make() creates a goroutine to monitor election timeout. If timeout elapses, it will make a new goroutine to run election. 

Start(cmd) creates a goroutine to handle proposal for cmd. It will create sub-goroutines, each for an RPC call to AppendEntries(). It exits when all RPC calls are complete. It can be cancelled after transition to follower.

Heartbeat-sending goroutine should start after transitioning to leader. It can be cancelled after transitioning to follower. It can cause a transition from leader to follower, at which point it should exit.

Winning election (from election timeout) creates goroutine that starts heartbeat goroutine. When heartbeat goroutine exits because of 