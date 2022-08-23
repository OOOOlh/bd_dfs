package hdfs

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
)

/** Configurations for Pseudo Distributed Mode **/

/** Configurations for ALL Mode **/
const SPLIT_UNIT int = 1000
const REDUNDANCE int = 2
const DN_CAPACITY int = 400
const DN_DIR string = "./datanode"
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

//type NameSpaceStruct FileFolderNode

// 创建文件目录方法  input-> (文件当前目录， 新建文件夹名字)
// 创建成功返回True  创建失败（当前目录不存在， 或者该文件夹已经存在）返回False
func (Node *Folder) CreateFolder(curPath string, folderName string) bool {
	path := strings.Split(curPath, "/")[1:]
	index := 0
	fmt.Println(len(Node.Folder))
	for index < len(path) {
		if Node.Name == path[index] {
			index++
			if index >= len(path) {
				for _, f := range Node.Folder {
					if f.Name == folderName {
						return false
					}
				}
				Node.Folder = append(Node.Folder, &Folder{
					folderName,
					[]*Folder{},
					[]*File{},
				})
				fmt.Println(len(Node.Folder))
				return true
			}
			for _, node := range Node.Folder {
				if node.Name == path[index] {
					Node = node
					index++
					if index >= len(path) {
						for _, f := range Node.Folder {
							if f.Name == folderName {
								return false
							}
						}
						Node.Folder = append(Node.Folder, &Folder{
							folderName,
							[]*Folder{},
							[]*File{},
						})
						return true
					}
				}
			}
		}
	}
	return false
}

// DataNode的TreeStruct
type Folder struct {
	Name string
	//子文件夹
	Folder []*Folder
	//子文件
	Files []*File
}

type File struct {
	Name            string
	Length          int64
	Chunks          []FileChunk
	OffsetLastChunk int
	Info            string // file info

	RemotePath string
}

// 修改目录名
func (Node *Folder) ReNameFolderName(preFolder string, reNameFolder string) bool {
	path := strings.Split(preFolder, "/")[1:]
	prePath := path[:len(path)-1]
	preFolderName := path[len(path)-1]
	newFolderName := reNameFolder
	index := 0
	for index < len(prePath) {
		if Node.Name == prePath[index] {
			index++
			if index >= len(prePath) {
				for _, item := range Node.Folder {
					if item.Name == preFolderName {
						item.Name = newFolderName
						return true
					}
				}
				return false
			}
			for _, node := range Node.Folder {
				if node.Name == prePath[index] {
					Node = node
					index++
					if index >= len(prePath) {
						for _, item := range Node.Folder {
							if item.Name == preFolderName {
								item.Name = newFolderName
								return true
							}
						}
						return false
					}
				}
			}
		}
	}
	return false
}

//  /root/hdfs/dn/nn/
// 根据目录结构查找文件列表
func (Node *Folder) GetFileList(filePath string) ([]*File, []*Folder) {
	path := strings.Split(filePath, "/")[1:]
	index := 0
	for index < len(path) {
		if Node.Name == path[index] {
			index++
			if index >= len(path) {
				return Node.Files, Node.Folder
			}
			for _, node := range Node.Folder {
				if node.Name == path[index] {
					Node = node
					index++
					if index >= len(path) {
						return Node.Files, Node.Folder
					}
				}
			}
		}
	}
	return nil, nil
}

// 根据目录获取文件节点信息
func (Node *Folder) GetFileNode(filePath string) (*File, error) {
	// /root/folder1/data.txt  -> [root, folder1, data.txt]
	path := strings.Split(filePath, "/")[1:]
	if path[0] != Node.Name {
		return nil, fmt.Errorf("node name mismatch")
	}
	node := Node
	for _, step := range path[1 : len(path)-1] {
		validFolder := false
		for _, folder := range node.Folder {
			if folder.Name == step {
				node = folder
				validFolder = true
				break
			}
		}
		// 没有该目录
		if !validFolder {
			return nil, fmt.Errorf("folder=%v not found", step)
		}
	}
	for _, file := range node.Files {
		if file.Name == path[len(path)-1] {
			return file, nil
		}
	}
	//没有该文件
	return nil, fmt.Errorf("file=%v not found", path[len(path)-1])
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
	NameSpace *Folder
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

	StandByDataNode [][]string
}
type DataNode struct {
	Location     string `json:"Location"` // http://IP:Port/
	Port         int    `json:"Port"`
	StorageTotal int    `json:"StorageTotal"` // a chunk as a unit
	StorageAvail int    `json:"StorageAvail"`
	ChunkAvail   []int  `json:"ChunkAvail"` //空闲块表
	LastEdit     int64  `json:"LastEdit"`
	DATANODE_DIR string `json:"DATANODE_DIR"`
	// Ticker *time.Ticker
	NNLocation []string
	LastQuery int
	// DNLogger *log.Logger
	ZapLogger *zap.SugaredLogger
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
