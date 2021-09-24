# Distributed-Proof-Of-Work

This is a distributed proof of work which involves clients, a coordinator, and workers as seperate nodes.
Tracing is recorded through the tracing server. The nodes communicate through go lang RPC's, and caching is used at the coordinator
and worker nodes to imporve performance. All nodes appear under the */cmd* folder.

Channels are used to communicate between threads, and RPC's are used to communicate between nodes.

## Clients:
Clients make independent RPC requests to a coordinator to determine the proof of work.

## Coordinator:
Coordinator receives requests from clients and checks if the result is in the cache. If not,
it will asynchronously call the workers to determine the proof of work. It will then recieve results and 
coordinate shutting down all workers and updating the cache before returning the result.

## Worker:
Workers are assigned a certain search space depending on the byte prefix when performing the proof of work.
Workers also contain a cache.

*UBC CPSC 416 Assignment*
