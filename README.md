# bd_hdfs
Implementation of simple distributed storage system
### 1. 基本模块功能介绍  

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

### 2. 项目启动流程  

```
cd nn1 
go run NN.go //启动namenode和多个datanode
(当前目录下)
cd ../client_main  
(调用客户端发出执行，以上传文件为例)
go run Client.go  -local <本地文件路径> -remote <远程文件路径>
```
