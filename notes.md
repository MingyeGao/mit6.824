# Lab 1
1. Master向Worker传递的是什么？
* 传递的不是数据，而是任务。
* 所有的任务，在确定输入（所有文件和nReduce）时就已经确定了。
* 真实生产环境中，可以根据Worker本地存储的文件调度任务，降低传输开销。

2. 中间文件mr-X-Y的意义？
* X，Y都是任务ID，不是WorkerID

3. 如何重试？
* 最初我使用的方式是："判断一个Worker是否10s内，未与Master通信。如果是，则将此Worker上承载的任务重试"。
* 实践过程中发现，有时Worker会"吞任务"：10s内与Master一直通信，但没有返回任务结果。
* 最终变成了监控任务：如果一个任务在10s中未完成，则将该任务分配至空闲Worker重试。

# Lab 2A

1. 如果集群中有服务宕机，Candidate如何获取集群中可用Server的数量？
* 可以通过rpc是否超时判断集群中有效Server的数量。

2. 如果一个集群中只剩下一个Server，那么他以什么状态运行？
* Follower

## 一些坑

1. 发送rpc时要手动设置超时时间。
* 虽然注释上写了*Thus there is no need to implement your own timeouts around Call().*，
但是在labrpc源码实现中，如果服务器不可用，则超时时间最高可达7s(labrpc L296)。
所以还是要手动设置一个alarmer的，如果超时立即返回。

2. 发送SendRequest rpc要使用goroutine并行发送。

3. 发送rpc时要解锁，否则可能导致死锁。