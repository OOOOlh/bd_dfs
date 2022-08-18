package hdfs

import (
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"net/http"
	"os"
	"strconv"
	"strings"
)

/*
	未添加功能：
		防止重复
*/
func (namenode *NameNode) Run() {
	router := gin.Default()

	//router.POST("/put", func(c *gin.Context) {
	//	b, _ := c.GetRawData() // 从c.Request.Body读取请求数据
	//	file := &File{}
	//	// 反序列化
	//	if len(b) == 0 {
	//		fmt.Println("put request body为空")
	//	}
	//	if err := json.Unmarshal(b, file); err != nil {
	//		fmt.Println("namenode put json to byte error", err)
	//	}
	//	var chunkNum int
	//	var fileLength = int(file.Length)
	//	// chunkNum = file.Length/
	//	if file.Length%int64(SPLIT_UNIT) == 0 {
	//		chunkNum = fileLength / SPLIT_UNIT
	//		file.OffsetLastChunk = 0
	//	} else {
	//		chunkNum = fileLength/SPLIT_UNIT + 1
	//		file.OffsetLastChunk = chunkNum*SPLIT_UNIT - fileLength
	//	}
	//	for i := 0; i < int(chunkNum); i++ {
	//		replicaLocationList := namenode.AllocateChunk()
	//		fileChunk := &FileChunk{}
	//		file.Chunks = append(file.Chunks, *fileChunk)
	//		file.Chunks[i].ReplicaLocationList = replicaLocationList
	//	}
	//
	//	ns := namenode.NameSpace
	//	//对应每一个文件，一个文件对应一个命名空间
	//	ns[file.Name] = *file
	//	namenode.NameSpace = ns
	//
	//	c.JSON(http.StatusOK, file)
	//})
	//
	//router.GET("/getfile/:filename", func(c *gin.Context) {
	//	filename := c.Param("filename")
	//	fmt.Println("$ getfile ...", filename)
	//	file := namenode.NameSpace[filename]
	//
	//	c.JSON(http.StatusOK, file)
	//})
	//
	//router.GET("/delfile/:filename", func(c *gin.Context) {
	//	filename := c.Param("filename")
	//	fmt.Println("$ delfile ...", filename)
	//	file := namenode.NameSpace[filename]
	//	for i := 0; i < len(file.Chunks); i++ {
	//		namenode.DelChunk(file, i)
	//	}
	//	c.JSON(http.StatusOK, file)
	//})

	router.GET("/getfolder/:foldername", func(c *gin.Context) {
		foldername := c.Param("foldername")
		fmt.Println("$ getfolder ...", foldername)
		files := namenode.NameSpace.GetFileList(foldername)
		var filenames []string
		for i := 0; i < len(files); i++ {
			filenames = append(filenames, files[i].Name)
		}
		c.JSON(http.StatusOK, filenames)
	})

	router.Run(":" + strconv.Itoa(namenode.Port))
}

func (namenode *NameNode) DelChunk(file File, num int) {
	//预删除文件的块信息
	//修改namenode.DataNodes[].ChunkAvail
	//和namenode.DataNodes[].StorageAvail
	for i := 0; i < REDUNDANCE; i++ {
		chunklocation := file.Chunks[num].ReplicaLocationList[i].ServerLocation
		chunknum := file.Chunks[num].ReplicaLocationList[i].ReplicaNum

		index := namenode.Map[chunklocation]
		namenode.DataNodes[index].ChunkAvail = append(namenode.DataNodes[index].ChunkAvail, chunknum)
		namenode.DataNodes[index].StorageAvail++
	}
}

func (namenode *NameNode) AllocateChunk() (rlList [REDUNDANCE]ReplicaLocation) {
	redundance := namenode.REDUNDANCE
	var max [REDUNDANCE]int
	for i := 0; i < redundance; i++ {
		max[i] = 0
		//找到目前空闲块最多的NA
		for j := 0; j < namenode.DNNumber; j++ {
			//遍历每一个DN，找到空闲块最多的前redundance个DN
			if namenode.DataNodes[j].StorageAvail > namenode.DataNodes[max[i]].StorageAvail {
				max[i] = j
			}
		}

		//ServerLocation是DN地址
		rlList[i].ServerLocation = namenode.DataNodes[max[i]].Location
		//ReplicaNum是DN已用的块
		rlList[i].ReplicaNum = namenode.DataNodes[max[i]].ChunkAvail[0]
		n := namenode.DataNodes[max[i]].StorageAvail

		namenode.DataNodes[max[i]].ChunkAvail[0] = namenode.DataNodes[max[i]].ChunkAvail[n-1]
		namenode.DataNodes[max[i]].ChunkAvail = namenode.DataNodes[max[i]].ChunkAvail[0 : n-1]
		namenode.DataNodes[max[i]].StorageAvail--
	}

	return rlList
}

func (namenode *NameNode) Reset() {
	// CleanFile("TinyDFS/DataNode1/chunk-"+strconv.Itoa(i))
	fmt.Println("# Reset...")

	err := os.RemoveAll(namenode.NAMENODE_DIR + "/")
	if err != nil {
		fmt.Println("XXX NameNode error at RemoveAll dir", err.Error())
		TDFSLogger.Fatal("XXX NameNode error: ", err)
	}

	err = os.MkdirAll(namenode.NAMENODE_DIR, 0777)
	if err != nil {
		fmt.Println("XXX NameNode error at MkdirAll", err.Error())
		TDFSLogger.Fatal("XXX NameNode error: ", err)
	}
}

func (namenode *NameNode) SetConfig(location string, dnnumber int, redundance int, dnlocations []string) {
	temp := strings.Split(location, ":")
	res, err := strconv.Atoi(temp[2])
	if err != nil {
		fmt.Println("XXX NameNode error at Atoi parse Port", err.Error())
		TDFSLogger.Fatal("XXX NameNode error: ", err)
	}
	ns := Folder{}
	namenode.NameSpace = ns
	namenode.Port = res
	namenode.Location = location
	namenode.DNNumber = dnnumber
	namenode.DNLocations = dnlocations
	namenode.REDUNDANCE = redundance
	fmt.Println("************************************************************")
	fmt.Println("************************************************************")
	fmt.Printf("*** Successfully Set Config data for the namenode\n")
	namenode.ShowInfo()
	fmt.Println("************************************************************")
	fmt.Println("************************************************************")
}

func (namenode *NameNode) ShowInfo() {
	fmt.Println("************************************************************")
	fmt.Println("****************** showinf for NameNode ********************")
	fmt.Printf("Location: %s\n", namenode.Location)
	fmt.Printf("DATANODE_DIR: %s\n", namenode.NAMENODE_DIR)
	fmt.Printf("Port: %d\n", namenode.Port)
	fmt.Printf("DNNumber: %d\n", namenode.DNNumber)
	fmt.Printf("REDUNDANCE: %d\n", namenode.REDUNDANCE)
	fmt.Printf("DNLocations: %s\n", namenode.DNLocations)
	fmt.Printf("DataNodes: ")
	fmt.Println(namenode.DataNodes)
	fmt.Println("******************** end of showinfo ***********************")
	fmt.Println("************************************************************")
}

func (namenode *NameNode) GetDNMeta() { // UpdateMeta
	namenode.Map = make(map[string]int)
	for i := 0; i < len(namenode.DNLocations); i++ {
		namenode.Map[namenode.DNLocations[i]] = i
		response, err := http.Get(namenode.DNLocations[i] + "/getmeta")
		if err != nil {
			fmt.Println("XXX NameNode error at Get meta of ", namenode.DNLocations[i], ": ", err.Error())
			TDFSLogger.Fatal("XXX NameNode error: ", err)
		}
		defer response.Body.Close()

		var dn DataNode
		err = json.NewDecoder(response.Body).Decode(&dn)
		if err != nil {
			fmt.Println("XXX NameNode error at decode response to json.", err.Error())
			TDFSLogger.Fatal("XXX NameNode error: ", err)
		}
		// fmt.Println(dn)
		// err = json.Unmarshal([]byte(str), &dn)
		namenode.DataNodes = append(namenode.DataNodes, dn)
	}
	namenode.ShowInfo()
}
