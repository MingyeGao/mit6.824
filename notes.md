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
1. 如果一个集群中只剩下一个Server，那么他以什么状态运行？
* Follower

## 一些坑

1. 发送rpc时要手动设置超时时间。
* 虽然注释上写了*Thus there is no need to implement your own timeouts around Call().*，
但是在labrpc源码实现中，如果服务器不可用，则超时时间最高可达7s(labrpc L296)。
所以还是要手动设置一个alarmer的，如果超时立即返回。

2. 发送SendRequest rpc要使用goroutine并行发送。

3. 发送rpc时要解锁，否则可能导致死锁。

# Lab3A
## 如何保证命令的幂等性

一个指令多次执行，得到的效果应该和该指令只执行过一次一样。

客户端发送的PutAppend消息被成功接收后可能收不到对应的响应。

客户端发送的Get指令可能重发，如果实时读取数据，则可能和第一次Get的结果不一样。

如果客户端重复请求，会导致统一指令被重复执行，因此需要保证幂等。

我的实现方式是：客户端每次请求带一个id，如果服务端判断这个id存在了，则返回之前这个ID对应的结果；否则就按第一次接受该指令处理。

## crash时如何恢复
每次从ApplyCh读取数据时，需要判断：处理完这个命令后，是否需要向一个rpc请求作响应。

我的实现方式是：每次kvServer启动时，都赋给它一个uniqueID。

服务再次启动时，uniqueID不会相同。

处理请求时，在op中带上uniqueID；从applyCh读取数据时，检查op中带有的uniqueID和本server的uniqueID是否相同。

如果相同，则表明该命令是由一个打到leader上的rpc请求产生的，需要回复；

否则说明server不是leader，或者当前的op是因为宕机重放产生的，不需要回复。
