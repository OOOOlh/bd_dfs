package hdfs

import (
	"fmt"
	"strings"
)

/** Configurations for Pseudo Distributed Mode **/

/** Configurations for ALL Mode **/
const SPLIT_UNIT int = 1000
const REDUNDANCE int = 2
// const CHUNKTOTAL int = 400

// Chunk 一律表示逻辑概念，表示文件块
// Replica 表示文件块副本，是实际存储的
/** Data Structure **/
type ChunkUnit []byte // SPLIT_UNIT
// type ChunkReplicaOfFile map[int]FileChunk

// type FileChunk struct{
// 	Filename string
// 	ChunkNum int
// }

//父文件夹
// DataNode的TreeStruct
type Folder struct {
	Name   string
	//子文件夹
	Folder []*Folder
	//子文件
	Files  []*File
}

type File struct {
	Name            string
	Length          int64
	Chunks          []FileChunk
	OffsetLastChunk int
	Info            string // file info

	RemotePath string
}


//  /root/hdfs/dn/nn/
// 根据目录结构查找文件列表
func (Node *Folder) GetFileList(filePath string) []*File {
	path := strings.Split(filePath, "/")[1:]
	index := 0
	for index < len(path) {
		if Node.Name == path[index] {
			index++
			if index >= len(path) {
				return Node.Files
			}
			for _, node := range Node.Folder {
				if node.Name == path[index] {
					Node = node
					index++
					if index >= len(path) {
						return Node.Files
					}
				}
			}
		}
	}
	return nil
}

// 根据目录获取文件节点信息
func (Node *Folder) GetFileNode(filePath string) (*File, error) {
	// root/folder1/data.txt  -> [root, folder1, data.txt]
	path := strings.Split(filePath, "/")
	if path[0] != Node.Name {
		return nil, fmt.Errorf("node name mismatch")
	}
	node := Node
	for _, step := range path[1 : len(path)-1] {
		for _, folder := range node.Folder {
			if folder.Name == step {
				node = folder
				break
			}
		}
		// 没有该目录
		return nil, fmt.Errorf("folder not found")
	}
	for _, file := range Node.Files {
		if file.Name == path[len(path)-1] {
			return file, nil
		}
	}
	//没有该文件
	return nil, fmt.Errorf("file not found")
}

type FileToDataNode struct {
	Block []int
}

type FileChunk struct {
	Info                string // checksum
	ReplicaLocationList [REDUNDANCE]ReplicaLocation
}
type ReplicaLocation struct {
	//冗余块的位置
	ServerLocation string
	ReplicaNum     int
}

type Client struct {
	StoreLocation     string
	TempStoreLocation string
	NameNodeAddr      string
	Mode              int
}

type Config struct {
	NameNodeAddr string
}

//限制文件夹层数为3
//最长比如是/root/bd_hdfs/auto.png
type NameNode struct {
// <<<<<<< HEAD
// 	FsImage Folder
// =======
	NameSpace *Folder
// >>>>>>> 16b5f6a15561897c5099d22520d9386b4bfe1800
	Location  string
	Port      int
	//DataNode数量
	DNNumber int
	//DataNode位置
	DNLocations []string
	// 保存各个DataNode的数据块消息
	DataNodes    []DataNode
	NAMENODE_DIR string
	// 冗余块
	REDUNDANCE int
	Map        map[string]int
}
type DataNode struct {
	Location     string `json:"Location"` // http://IP:Port/
	Port         int    `json:"Port"`
	StorageTotal int    `json:"StorageTotal"` // a chunk as a unit
	StorageAvail int    `json:"StorageAvail"`
	ChunkAvail   []int  `json:"ChunkAvail"` //空闲块表
	LastEdit     int64  `json:"LastEdit"`
	DATANODE_DIR string `json:"DATANODE_DIR"`
}
type DNMeta struct {
	StorageTotal int `json:"StorageTotal"`
	StorageAvail int
	ChunkAvail   []int
	LastEdit     int64
}

func (conf *Config) Set(addr string) {
	conf.NameNodeAddr = addr
}
