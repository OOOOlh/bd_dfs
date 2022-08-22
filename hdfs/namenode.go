package hdfs

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/gin-gonic/gin"
)

func (namenode *NameNode) MonitorDN(){
	defer func () {
		if x := recover(); x != nil{
			TDFSLogger.Fatalf("panic when monitor DataNode, err: %v\n", x) 
		}
	}()

	go func ()  {
		for{
			for i := 0; i < len(namenode.DataNodes); i++ {
				t := time.Now().Minute()
				//如果大于两分钟，就表示该DN出现问题，无法完成上报任务。新建一个节点，将所有数据复制到新节点上
				if (t - namenode.DataNodes[i].LastQuery) > 2 {

				}
			}
		}
	}()
}


func (namenode *NameNode) Run() {
	namenode.MonitorDN()
	router := gin.Default()
	router.Use(MwPrometheusHttp)
	// register the `/metrics` route.
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))


	//校验dn信息
	router.POST("/heartbeat", func(c *gin.Context)  {
		d, _ := c.GetRawData()
		datanode := DataNode{}
		// 反序列化
		if len(d) == 0 {
			fmt.Println("put request body为空")
		}
		if err := json.Unmarshal(d, &datanode); err != nil {
			fmt.Println("namenode put json to byte error", err)
		}

		localDataNode := &namenode.DataNodes[namenode.Map[datanode.Location]]
		localDataNode.LastQuery = time.Now().Minute()

		//可用chunk数
		if(len(datanode.ChunkAvail) != len(localDataNode.ChunkAvail)){
			TDFSLogger.Fatalf("datanode %s : 可用chunk数目出错\n", datanode.Location)
		}

	})


	router.POST("/put", func(c *gin.Context) {
		b, _ := c.GetRawData() // 从c.Request.Body读取请求数据
		file := &File{}
		// 反序列化
		if len(b) == 0 {
			fmt.Println("put request body为空")
		}
		if err := json.Unmarshal(b, file); err != nil {
			fmt.Println("namenode put json to byte error", err)
		}

		// 去除最开始的斜杠
		path := strings.Split(file.RemotePath, "/")

		var n *Folder
		ff := namenode.NameSpace
		//例如：path = /root/temp/dd/
		//遍历所有文件夹，/root/下的所有文件夹
		folder := &ff.Folder
		// folder := &namenode.NameSpace.Folder
		for _, p := range path[1:] {
			if p == ""{
				continue
			}
			//fmt.Println(p)
			exist := false
			for _, n = range *folder {
				if p == n.Name {
					exist = true
					break
				}
			}
			//如果不存在，就新建一个文件夹
			if !exist {
				TDFSLogger.Println("namenode: file not exist")
				var tempFloder Folder = Folder{}
				tempFloder.Name = p
				*folder = append(*folder, &tempFloder)
				//下一层
				folder = &(*folder)[len(*folder)-1].Folder
				n = &tempFloder
			} else {
				folder = &n.Folder
			}

		}

		//直接把文件写在当前文件夹下
		var exist bool
		var changed bool = true
		var f *File
		for _, f = range n.Files {
			exist = false
			//找到目标文件
			if f.Name == file.Name {
				exist = true
				//校验文件是否改变
				if f.Info == file.Info {
					//如果没改变，client就不用向datanode改变信息
					TDFSLogger.Println("namenode: file exists and not changed")
					changed = false
				}
				break
			}
		}

		var chunkNum int
		var fileLength = int(file.Length)
		if file.Length%int64(SPLIT_UNIT) == 0 {
			chunkNum = fileLength / SPLIT_UNIT
			file.OffsetLastChunk = 0
		} else {
			chunkNum = fileLength/SPLIT_UNIT + 1
			file.OffsetLastChunk = chunkNum*SPLIT_UNIT - fileLength
		}
		for i := 0; i < int(chunkNum); i++ {
			replicaLocationList := namenode.AllocateChunk()
			fileChunk := &FileChunk{}
			file.Chunks = append(file.Chunks, *fileChunk)
			file.Chunks[i].ReplicaLocationList = replicaLocationList
		}

		if !exist {
			n.Files = append(n.Files, file)
		} else if changed {
			TDFSLogger.Println("namenode: file exists and changed")
			f = file
		}
		if !changed {
			file = &File{}
		}
		c.JSON(http.StatusOK, file)
	})
	//
	router.GET("/getfile", func(c *gin.Context) {
		filename := c.Query("filename")
		fmt.Println("$ getfile ...", filename)
		TDFSLogger.Println("filename")
		node := namenode.NameSpace
		file, err := node.GetFileNode(filename)
		if err != nil {
			TDFSLogger.Printf("get file=%v error=%v\n", filename, err.Error())
			fmt.Printf("get file=%v error=%v\n", filename, err.Error())
			c.JSON(http.StatusNotFound, err.Error())
			return
		}
		c.JSON(http.StatusOK, file)
	})

	router.GET("/delfile/:filename", func(c *gin.Context) {
		filename := c.Param("filename")
		fmt.Println("$ delfile ...", filename)
		var targetFile *File = nil
		files := namenode.NameSpace.Files
		for i := 0; i < len(files); i++ {
			if files[i].Name == filename {
				targetFile = files[i]
				for j := 0; j < len(targetFile.Chunks); j++ {
					namenode.DelChunk(*targetFile, j)
				}
			}
		}

		c.JSON(http.StatusOK, targetFile)
	})

	// Folder ReName
	router.POST("/reFolderName", func(context *gin.Context) {
		b, _ := context.GetRawData() // 从c.Request.Body读取请求数据
		var dataMap map[string]string
		if err := json.Unmarshal(b, &dataMap); err != nil {
			fmt.Println("namenode put json to byte error", err)
		}
		res := namenode.NameSpace.ReNameFolderName(dataMap["preFolder"], dataMap["reNameFolder"])
		if res {
			context.JSON(http.StatusOK, 1)
		}
		context.JSON(http.StatusOK, -1)

	})

	//get Folders fromr cur path
	router.POST("/getFolders", func(context *gin.Context) {
		b, _ := context.GetRawData() // 从c.Request.Body读取请求数据
		var dataMap map[string]string
		if err := json.Unmarshal(b, &dataMap); err != nil {
			fmt.Println("namenode put json to byte error", err)
		}
		_, folders := namenode.NameSpace.GetFileList(dataMap["fname"])
		var filenames []string
		for i := 0; i < len(folders); i++ {
			filenames = append(filenames, folders[i].Name)
		}
		context.JSON(http.StatusOK, filenames)
	})
	// get Files from cur path
	router.POST("/getFiles", func(context *gin.Context) {
		b, _ := context.GetRawData() // 从c.Request.Body读取请求数据
		var dataMap map[string]string
		if err := json.Unmarshal(b, &dataMap); err != nil {
			fmt.Println("namenode put json to byte error", err)
		}
		files, _ := namenode.NameSpace.GetFileList(dataMap["fname"])
		var filenames []string
		for i := 0; i < len(files); i++ {
			filenames = append(filenames, files[i].Name)
		}
		context.JSON(http.StatusOK, filenames)
	})

	//router.GET("/getfolder/:foldername", func(c *gin.Context) {
	//	foldername := c.Param("foldername")
	//	fmt.Println("$ getfolder ...", foldername)
	//	TDFSLogger.Fatal("$ getfolder ...", foldername)
	//	files := namenode.NameSpace.GetFileList(foldername)
	//	var filenames []string
	//	for i := 0; i < len(files); i++ {
	//		filenames = append(filenames, files[i].Name)
	//	}
	//	c.JSON(http.StatusOK, filenames)
	//})

	//创建文件目录
	router.POST("/mkdir", func(context *gin.Context) {
		b, _ := context.GetRawData() // 从c.Request.Body读取请求数据
		var dataMap map[string]string
		if err := json.Unmarshal(b, &dataMap); err != nil {
			fmt.Println("namenode put json to byte error", err)
		}
		if namenode.NameSpace.CreateFolder(dataMap["curPath"], dataMap["folderName"]) {
			context.JSON(http.StatusOK, 1)
		}
		context.JSON(http.StatusOK, -1)
	})

	router.Run(":" + strconv.Itoa(namenode.Port))
}

func (namenode *NameNode) DelChunk(file File, num int) {
	//预删除文件的块信息
	//修改namenode.DataNodes[].ChunkAvail
	//和namenode.DataNodes[].StorageAvail
	var wg sync.WaitGroup
	wg.Add(REDUNDANCE)
	for i := 0; i < REDUNDANCE; i++ {
		go func(i int) {
			chunklocation := file.Chunks[num].ReplicaLocationList[i].ServerLocation
			chunknum := file.Chunks[num].ReplicaLocationList[i].ReplicaNum
			index := namenode.Map[chunklocation]
			namenode.DataNodes[index].ChunkAvail = append(namenode.DataNodes[index].ChunkAvail, chunknum)
			namenode.DataNodes[index].StorageAvail++
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func (namenode *NameNode) AllocateChunk() (rlList [REDUNDANCE]ReplicaLocation) {
	redundance := namenode.REDUNDANCE
	var max [REDUNDANCE]int
	var wg sync.WaitGroup
	wg.Add(REDUNDANCE)
	for i := 0; i < redundance; i++ {
		go func(i int) {
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
		}(i)
		wg.Done()
	}
	wg.Wait()
	return rlList
}

func (namenode *NameNode) SetConfig(location string, dnnumber int, redundance int, dnlocations []string) {
	temp := strings.Split(location, ":")
	res, err := strconv.Atoi(temp[2])
	if err != nil {
		fmt.Println("XXX NameNode error at Atoi parse Port", err.Error())
		TDFSLogger.Fatal("XXX NameNode error: ", err)
	}
	namenode.NameSpace = &Folder{
		Name:   "root",
		Folder: make([]*Folder, 0),
		Files:  make([]*File, 0),
	}
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
