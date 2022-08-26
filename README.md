# bd_hdfs
Implementation of simple distributed storage system


ByteDance Distributed File System（以下简称bd_dfs）是为完成**字节跳动青训营大项目设计的分布式文件系统**的名称。bd_dfs实现了在分布式文件系统中的文件上传Write、文件读取Read、文件删除Delete、文件元数据信息获取Stat, 文件目录创建Mkdir，文件重命名Rename, 文件目录信息List等基本的文件操作功能。从架构上，bd_dfs与HDFS/GFS有着类似的Master/Slave架构，bd_dfs包括客户端Client、名称节点NameNode（以下简称NN）、数据节点DataNode（以下简称DN）三部分构成。从编程实现上，我们使用Go语言编写了bd_dfs，并且使用了Go语言中的Gin框架来编写NN、DN服务器的后端。
### 1 基本架构

bd_dfs使用主从式（Master/Slave）架构。一个集群分为名称节点NN和多个数据节点DN。NN存放了文件目录和bd_dfs的全部控制信息。而DN存放所有文件的块副本数据及其哈希。

其中文件存储实现了分块存储：文件通过客户端发送给名称节点NN，然后NN对文件切分成等大小的文件块，然后把这些块发给特定的DN进行存储。而且还实现了冗余存储（bd_dfs的默认冗余系数是2）：一个文件块会被发给多个DN进行存储，这样在一份文件块数据失效之后，还有一块备份。冗余存储也是分布式文件系统中标配的。DN上不仅会存储文件块数据，还会存储文件块数据的哈希值（bd_dfs默认使用sha256哈希函数）。

另外文件读取实现了数据完整性检测。被分布式存储的文件在读取时NN会去相应的DN上进行读取文件块副本数据。然后NN会对返回的文件块副本数据进行校验和（Checksum）检测，即对其数据内容进行哈希计算，然后和存储在DN上的文件哈希进行对比，如果不一致说明数据块已经遭到破坏，此时NN会去其他DN上读取另一个块副本数据。

### 2 项目结构

在bd_dfs的项目下所有文件及文件夹如下所示：
```markdown
├── client_main
│   └── Client.go  // 来模拟一个d_dfs的Client
├── dn
│   └── DataNode // 在本地伪分布式演示时DN的工作目录
│       └── achunkhashs // DN的工作目录下存储文件块数据哈希值的目录
│   └── DN.go //启动一个bd_dfs的DataNode
├── dn1
│   └── DataNode1 // 在本地伪分布式演示时DN1的工作目录
│       └── achunkhashs // DN的工作目录下存储文件块数据哈希值的目录
│   └── DN1.go //启动一个bd_dfs的DataNode1
├── dn2
│   └── DataNode2 // 在本地伪分布式演示时DN2的工作目录
│       └── achunkhashs // DN的工作目录下存储文件块数据哈希值的目录
│   └── DN2.go //启动一个bd_dfs的DataNode2
├── dn3
│   └── DataNode3 // 在本地伪分布式演示时DN3的工作目录
│       └── achunkhashs // DN的工作目录下存储文件块数据哈希值的目
│   └── DN3.go //启动一个bd_dfs的DataNod3
├── hdfs
│   ├── TestConfig_test.go
│   ├── client.go // client相关的所有操作
│   ├── config.go // 系统的所有数据结构定义、参数相关
│   ├── datanode.go // datanode相关的所有操作
│   ├── metrics.go // 系统的监控函数定义
│   ├── namenode.go // namenode相关的所有操作
│   ├── utils.go / 文件操作的一些工具函数
│   └── zaplog.go // 系统日志的定义
├── nn
│   └──  NameNode  // 在本地伪分布式演示时NN的工作目录
│   └── NN.go
└── test.go

### 3. 基本模块功能介绍
1. 获取文件列表：获取指定文件夹下对应的文件列表  
`go run Client.go -filesNameOfGet <文件夹>`  
2. 获取目录列表： 获取指定文件夹下的目录列表   
`go run Client.go -foldersNameOfGet <文件夹>`
3. 目录路径重命名： 对目录进行重命名  
`go run Client.go -curFolder <文件夹路径> -reNameFolder <新命名>`
4. 在指定文件夹下创建新的目录  
`go run Client.go -curFolder <文件夹> -newFolder <新的文件夹名称>`
5. 节点扩容： 根据node名称和端口在本地开启新的datanode    
`go run Client.go -newNodeDir <datanode地址> -newNodePort <datanode端口>`  
6. 

### 4. 项目启动流程  

```
cd nn1 
go run NN.go //启动namenode和多个datanode
cd nn2
go run NN.go //启动namenode2
cd nn3
go run NN.go //启动namenode3
(当前目录下)
cd ../client_main  
(调用客户端发出执行，以上传文件为例)
go run Client.go  -local <本地文件路径> -remote <远程文件路径>
```
