# raft
my implementation of the distributed consensus algorithm (Raft) in Go

# How Raft works to support your replicated state machine design 
![alt text](image-4.png)

# When to use Raft ?
- Raft is not designed for highly throughput systems, because basically raft is a strong-leader algorithm.
- use raft for `Replicating configs for a distributed system cluster` for example, but don't use it for a `database implementation`, because it will be really slow when the load increase`.

#### How I've choosed the randomied election timeout ?
- Simply, from Raft paper : 
> Raft uses randomized election timeouts to ensure that
split votes are rare and that they are resolved quickly. To
prevent split votes in the first place, election timeouts are
chosen randomly from a fixed interval (e.g., 150–300ms).

# What is implemented so far:
1. Leader Election 
2. Log Replication 
3. Log consistency between nodes 
4. Node Stat Transition
5. Term Managment.
6. Paper's `RequestVote` and `AppendEntries` rpcs

# Missing features for future work:
1. Snapshotting algorithm
2. State Persistent on disk in case of crashing
3. cluster membership configurations changes 
4. client interaction to the raft pkg 
5. optimization to the AppendEntry rounds