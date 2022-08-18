package hdfs

import (
	"strings"
)

/** Configurations for Pseudo Distributed Mode **/

/** Configurations for ALL Mode **/
const SPLIT_UNIT int = 1000
const REDUNDANCE int = 2
const CHUNKTOTAL int = 100

// Chunk 一律表示逻辑概念，表示文件块
// Replica 表示文件块副本，是实际存储的
/** Data Structure **/
type ChunkUnit []byte // SPLIT_UNIT
// type ChunkReplicaOfFile map[int]FileChunk

// type FileChunk struct{
// 	Filename string
// 	ChunkNum int
// }

// DataNode的TreeStruct
type Folder struct {
	Name   string
	Folder []*Folder
	Files  []*File
}

type File struct {
	Name            string
	Length          int64
	Chunks          []FileChunk
	OffsetLastChunk int
	Info            string // file info
}

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
func (Node *Folder) GetFileNode(filePath string) *File {
	// /root/folder1/data.txt  -> [root, folder1, data.txt]
	path := strings.Split(filePath, "/")[1:]
	if path[0] != "root" {
		return nil
	}
	for _, step := range path[1 : len(path)-1] {
		flag := false
		for _, folder := range Node.Folder {
			if folder.Name == step {
				Node = folder
				flag = true
				break
			}
		}
		// 没有该目录
		if flag {
			return nil
		}
	}
	for _, file := range Node.Files {
		if file.Name == strings.Split(path[len(path)-1], ".")[0] {
			return file
		}
	}
	//没有该文件
	return nil
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

type NameNode struct {
	NameSpace Folder
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
