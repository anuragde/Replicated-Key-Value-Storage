# Replicated Key-Value Storage
Simplified version of the Amazon DynamoDB (Distributed Systems PA-4)

Implemented a Dynamo-style key-value storage, providing availability and linearizability at the same time.
- Implemented partitioning, replication, and failure handling.

1. PA Specification: https://docs.google.com/document/d/1VpTvRTb7TETtN59ovdfb1FMQDRXfq6H5Toh7L7Dq1P4/edit
2. Testing scripts and instructions are provided in the specification document.

#### Implementation:
Implementation always performs read and write operations successfully even under failures (Availability). At the same time, a read operation always returns the most recent value (Linearizability).

##### Partitioning:
Partitioning is implemented from the previous assignment on Simple DHT based on Chord protocol.

##### Replication:

- Insert requests when sent to the node not responsible for the key are sent to the leader replica responsible for the key which then sends the inserts requests to all the replica's (two successor nodes) and only after successful chain replication, an insert success is returned.

- File version is appended to the file during an Insert and is maintained locally at each node to achieve linearizability.

- Insert request failures to the nodes are sent to the two predecessor nodes.

- When a node recovers from failure, it sends a missed data request to the predecessor two nodes to be in-sync, and compares any missed data to its local data and updates it's records incase of missing records.

- Query requests are placed in a LinkedBlockingQueue and the data is returned to the requesting node when the record in the queue is updated.

- Delete requests are forwarded to all the nodes and returned upon success, logged for failure.


