# bd_dfs

Implementation of simple distributed storage system

ByteDance Distributed File System（以下简称 bd_dfs）是为完成**字节跳动青训营大项目设计的分布式文件系统**的名称。bd_dfs 实现了在分布式文件系统中的文件上传、文件读取、文件删除 、文件元数据信息获取, 文件目录创建，文件重命名, 文件目录信息等基本的文件操作功能。从架构上，bd_dfs 与 HDFS/GFS 有着类似的 Master/Slave 架构，bd_dfs 包括客户端 Client、名称节点 NameNode（以下简称 NN）、数据节点 DataNode（以下简称 DN）三部分构成。从编程实现上，我们使用 Go 语言编写了 bd_dfs，并且使用了 Go 语言中的 Gin 框架来编写 NN、DN 服务器的后端。

### 1 基本架构

bd_dfs 使用主从式（Master/Slave）架构。一个集群分为名称节点 NN 和多个数据节点 DN。NN 存放了文件目录和 bd_dfs 的全部控制信息。而 DN 存放所有文件的块副本数据及其哈希。

其中文件存储实现了分块存储：文件通过客户端发送给名称节点 NN，然后 NN 对文件切分成等大小的文件块，然后把这些块发给特定的 DN 进行存储。而且还实现了冗余存储（bd_dfs 的默认冗余系数是 2）：一个文件块会被发给多个 DN 进行存储，这样在一份文件块数据失效之后，还有一块备份。冗余存储也是分布式文件系统中标配的。DN 上不仅会存储文件块数据，还会存储文件块数据的哈希值（bd_dfs 默认使用 sha256 哈希函数）。

另外文件读取实现了数据完整性检测。被分布式存储的文件在读取时 NN 会去相应的 DN 上进行读取文件块副本数据。然后 NN 会对返回的文件块副本数据进行校验和（Checksum）检测，即对其数据内容进行哈希计算，然后和存储在 DN 上的文件哈希进行对比，如果不一致说明数据块已经遭到破坏，此时 NN 会去其他 DN 上读取另一个块副本数据。

### 2 项目结构

在 bd_dfs 的项目下所有文件及文件夹如下所示：

```markdown
├── client_main
│ └── Client.go // 来模拟一个 bd_dfs 的 Client
├── dn
│ └── DataNode // 在本地伪分布式演示时 DN 的工作目录
│ └── achunkhashs // DN 的工作目录下存储文件块数据哈希值的目录
│ └── DN.go //启动一个 bd_dfs 的 DataNode
├── dn1
│ └── DataNode1 // 在本地伪分布式演示时 DN1 的工作目录
│ └── achunkhashs // DN 的工作目录下存储文件块数据哈希值的目录
│ └── DN1.go //启动一个 bd_dfs 的 DataNode1
├── dn2
│ └── DataNode2 // 在本地伪分布式演示时 DN2 的工作目录
│ └── achunkhashs // DN 的工作目录下存储文件块数据哈希值的目录
│ └── DN2.go //启动一个 bd_dfs 的 DataNode2
├── dn3
│ └── DataNode3 // 在本地伪分布式演示时 DN3 的工作目录
│ └── achunkhashs // DN 的工作目录下存储文件块数据哈希值的目
│ └── DN3.go //启动一个 bd_dfs 的 DataNod3
├── hdfs
│ ├── TestConfig_test.go
│ ├── client.go // client 相关的所有操作
│ ├── config.go // 系统的所有数据结构定义、参数相关
│ ├── datanode.go // datanode 相关的所有操作
│ ├── metrics.go // 系统的监控函数定义
│ ├── namenode.go // namenode 相关的所有操作
│ ├── utils.go / 文件操作的一些工具函数
│ └── zaplog.go // 系统日志的定义
├── nn
│ └── NameNode // 在本地伪分布式演示时 NN 的工作目录
│ └── NN.go
└── test.go
```

### 3. 基本模块功能介绍及命令

1. 上传文件：将本地文件上传至分布式文件系统
   `go run Client.go -local <本地文件路径> -remote <远程文件路径>`
2. 下载文件：将分布式文件系统文件下载至本地
   `go run Client.go -getfile <远程文件路径>`
3. 删除文件：将分布式文件系统指定文件删除
   `go run Client.go -delfile <远程文件路径>`
4. 获取文件列表：获取指定文件夹下对应的文件列表  
   `go run Client.go -filesNameOfGet <文件夹>`
5. 获取目录列表： 获取指定文件夹下的目录列表  
   `go run Client.go -foldersNameOfGet <文件夹>`
6. 目录路径重命名： 对目录进行重命名  
   `go run Client.go -curFolder <文件夹路径> -reNameFolder <新命名>`
7. 在指定文件夹下创建新的目录  
   `go run Client.go -curFolder <文件夹> -newFolder <新的文件夹名称>`
8. 节点扩容： 根据 node 名称和端口在本地开启新的 datanode  
   `go run Client.go -newNodeDir <datanode地址> -newNodePort <datanode端口>`

### 4. 项目启动流程

```
  cd nn1
  go run NN.go //启动 namenode 和多个 datanode

  cd nn2
  go run NN.go //启动 namenode2

  cd nn3
  go run NN.go //启动 namenode3

  (当前目录下)
  cd ../client_main
  (调用客户端发出执行，以上传文件为例)
  go run Client.go -local <本地文件路径> -remote <远程文件路径>
  其他命令可以参考3

```

### 5. 模拟 DataNode 故障

**请在至少上传一个文件后执行后面操作**

Windows 下：

- 打开任务管理器，可以看到如下 3 个 DataNode 进程：
  ![image](https://github.com/OOOOlh/bd_dfs/blob/main/image/windows_datanode1.png)
- 任选一个，右键`结束任务`。目前还剩下两个：
  ![image](https://github.com/OOOOlh/bd_dfs/blob/main/image/windows_datanode2.png)
- 此时下载文件，可以看到下载速度很慢，但仍然能够下载完，因为 Client 要到其他副本 DataNode 上下载文件块

- 等待 30s，NameNode 感知到 DataNode 节点故障。可以看到此时新启动了一个进程。下载文件，非常流畅，因为此时故障节点上所有文件信息都被拷贝到新节点上了。
  ![image](https://github.com/OOOOlh/bd_dfs/blob/main/image/windows_datanode3.png)

Linux 下：
