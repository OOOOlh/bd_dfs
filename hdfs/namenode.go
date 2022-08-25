package hdfs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/gin-gonic/gin"
)


var mu sync.Locker

func (namenode *NameNode) MonitorDN(){
	defer func () {
		if x := recover(); x != nil{
			sugarLogger.Errorf("panic when monitor DataNode, err: %v\n", x)
		}
	}()

	// sugarLogger.Info("开始监视DN")
	ticker := time.NewTicker(5 * time.Second)
	for{
		<- ticker.C
		for i := 0; i < len(namenode.DataNodes); i++ {
			t := time.Now().Unix() - namenode.DataNodes[i].LastQuery
			// sugarLogger.Infof("DN: %s, 对应的t为%d, 当前时间: %d, LastQuery: %d", namenode.DataNodes[i].Location, t, time.Now().Unix(), namenode.DataNodes[i].LastQuery)
			//如果大于一分钟，就表示该DN出现问题，无法完成上报任务。新建一个节点，将所有数据复制到新节点上
			flag := false
			if t > 30 && flag == false{
				namenode.Mu.Lock()
				flag = true
				sugarLogger.Warnf("超出规定时间间隔")
				
				sugarLogger.Warnf("%s节点超过规定时间间隔:%d, 开始建立新DataNode\n", namenode.DataNodes[i].Location, t)
				dataNode := namenode.DataNodes[i]
				s := namenode.StandByDataNode[0]
				//启动新的datanode节点
				namenode.StartNewDataNode(s)

				//向namenode中添加新的datanode节点
				//向新的dn发送元数据查询请求，返回的元数据保存
				newLocation := "http://localhost:" + s[4]
				sugarLogger.Infof("新的DataNode节点地址为%s\n", newLocation)
				//替换掉旧的节点，
				namenode.Map[newLocation] = i

				//新旧节点映射
				namenode.OldToNewMap[dataNode.Location] = newLocation
				
				response, err := http.Get(newLocation + "/getmeta")
				if err != nil {
					fmt.Println("XXX NameNode error at Get meta of ", newLocation, ": ", err.Error())
					sugarLogger.Errorf("get newDN error: %s", err)
				}
				defer response.Body.Close()

				var dn DataNode
				err = json.NewDecoder(response.Body).Decode(&dn)
				if err != nil {
					fmt.Println("XXX NameNode error at decode response to json.", err.Error())
					sugarLogger.Errorf("json decode error:%s", err)
				}
				dn.LastQuery = time.Now().Unix()

				sugarLogger.Infof("挂掉的节点的总容量为%d, 剩余容量为%d", dataNode.StorageTotal, dataNode.StorageAvail)
				//这一步只是为了通知其他DN给新的DN发送chunk，没有改变namenode内部的信息
				for i := 0; i < (dataNode.StorageTotal - dataNode.StorageAvail); i++{
					for j := 0; j < REDUNDANCE - 1; j++{
						//该DN也是快要挂掉的了，所以不用赋值给它
						copyChunkLocation := dataNode.ChunkCopy[i][j].ServerLocation
						copyChunkReplicaNum := dataNode.ChunkCopy[i][j].ReplicaNum

						//如果存在，就转成新地址
						if d, ok := namenode.OldToNewMap[copyChunkLocation]; ok{
							copyChunkLocation = d
						}

						//存储新的DN位置，和备份DN的chunk位置
						//将该信息发送给副本DN，让副本DN给新DN发送对应的chunk
						rm := &ReplicaLocation{
							ServerLocation: newLocation,
							ReplicaNum: copyChunkReplicaNum,
							OldNum: i,
						}

						d, err := json.Marshal(rm)
						if err != nil {
							fmt.Println("json to byte[] error", err)
						}
						// 序列化
						reader := bytes.NewReader(d)
						resp, err := http.Post(copyChunkLocation+"/fixchunk", "application/json", reader)
						if err != nil {
							fmt.Println("http post error", err)
						}

						/** Read response **/
						_, err = ioutil.ReadAll(resp.Body)
						if err != nil {
							fmt.Println("NameNode error at Read response", err.Error())
							sugarLogger.Errorf("read response error: %s", err)
						}
					}
				}
				//在namenode的datanode数组中旧的用新的代替
				namenode.DataNodes[i] = dn

				//用后即删
				namenode.StandByDataNode = namenode.StandByDataNode[1:]
				namenode.Mu.Unlock()
			}
			flag = false
		}
	}
}

func (namenode *NameNode) Run() {
	// namenode.MonitorDN()
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
		sugarLogger.Infof("收到来自datanode:%s的心跳", datanode.Location)

		localDataNode := &namenode.DataNodes[namenode.Map[datanode.Location]]
		namenode.Mu.Lock()
		localDataNode.LastQuery = time.Now().Unix()
		namenode.Mu.Unlock()

		//可用chunk数
		if(len(datanode.ChunkAvail) != len(localDataNode.ChunkAvail)){
			sugarLogger.Errorf("datanode %s : 可用chunk数目出错\n", datanode.Location)
		}
		c.String(http.StatusOK, "")
	})


	router.POST("/put", func(c *gin.Context) {
		b, _ := c.GetRawData() // 从c.Request.Body读取请求数据
		file := &File{}
		// 反序列化
		if len(b) == 0 {
			sugarLogger.Warn("client to namenode put request body为空")
		}
		if err := json.Unmarshal(b, file); err != nil {
			sugarLogger.Errorf("namenode put json to byte error: %s", err)
		}

		path := strings.Split(file.RemotePath, "/")

		var n *Folder
		ff := namenode.NameSpace
		//例如：path = /root/temp/dd/
		//遍历所有文件夹，/root/下的所有文件夹
		folder := &ff.Folder
		// folder := &namenode.NameSpace.Folder
		// /root或/root/都ok
		if len(path) == 2 || (len(path) == 3 && path[2] == ""){
			n = ff
		}else{
			for _, p := range path[2:] {
				if p == "" {
					continue
				}
				exist := false
				for _, n = range *folder {
					if p == n.Name {
						exist = true
						break
					}
				}
				//如果不存在，就新建一个文件夹
				if !exist {
					sugarLogger.Warn("namenode: file not exist")
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
					sugarLogger.Info("namenode: file exists and not changed")
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
			replicaLocationList, _ := namenode.AllocateChunk()
			// fmt.Println("rep", replicaLocationList)
			// fmt.Println("arr", arr)
			
			//replicaLocationList中记录的是该chunk所有的ReplicaLocation信息
			//arr记录的是含有chunk的datanode的下标
			//记录每个chunk的副本信息
			// i:第i个chunk
			// j:第j个副本
			//功能，为replicaLocationList中的所有DN中添加副本信息
			//所以需要遍历所有的DN
			//最外层找下标
			for j := 0; j < len(replicaLocationList); j++{
				index := replicaLocationList[j].index
				t := 0
				for k := 0; ; k++{
					if k == len(replicaLocationList){
						break
					}else if k == j{
						continue
					}
					namenode.DataNodes[index].ChunkCopy[replicaLocationList[j].ReplicaNum][t] = replicaLocationList[k]
					// sugarLogger.Infof("现节点为%s, 备份位置为%d, 备份节点为%s, 备份位置为%d", replicaLocationList[j].ServerLocation, replicaLocationList[j].ReplicaNum, replicaLocationList[k].ServerLocation, replicaLocationList[k].ReplicaNum)
					t = t + 1
				}
				
			}

			// for j := 0; j < len(arr); j++{
			// 	t := 0
			// 	for k := 0; ; k++{
			// 		if k == REDUNDANCE{
			// 			break
			// 		}else if k != j{
			// 			continue
			// 		}
			// 		//replicaLocationList[t].ReplicaNum指的是
			// 		namenode.DataNodes[arr[j]].ChunkCopy[replicaLocationList[k].ReplicaNum][t] = replicaLocationList[k]
			// 		sugarLogger.Infof("namenode.DataNodes[%d].chunkCopy[%d][%d]为, 记录的副本位置为:%s, 对应index为%d, RNum为%d", arr[j], replicaLocationList[t].ReplicaNum, t, namenode.DataNodes[arr[j]].ChunkCopy[i][t].ServerLocation, namenode.DataNodes[arr[j]].ChunkCopy[i][t].index, namenode.DataNodes[arr[j]].ChunkCopy[i][t].ReplicaNum)
			// 		t++
			// 		// namenode.DataNodes[arr[j]].ChunkCopy[i] = append(namenode.DataNodes[arr[j]].ChunkCopy[i], replicaLocationList[k])
			// 	}
			// }
			fileChunk := &FileChunk{}
			file.Chunks = append(file.Chunks, *fileChunk)
			file.Chunks[i].ReplicaLocationList = replicaLocationList
		}

		if !exist {
			n.Files = append(n.Files, file)
		} else if changed {
			sugarLogger.Info("namenode: file exists and changed")
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
		node := namenode.NameSpace
		file, err := node.GetFileNode(filename)
		if err!= nil{
			fmt.Println(err)
		}

		//遍历每个chunk的每个副本位置，并更新
		//因为可能该节点挂了，需要更新
		//不确定是否能够到文件树内部的信息
		for i := 0; i < len(file.Chunks); i++{
			for j := 0; j < len(file.Chunks[i].ReplicaLocationList); j++{
				file.Chunks[i].ReplicaLocationList[j].ServerLocation = namenode.DataNodes[file.Chunks[i].ReplicaLocationList[j].index].Location
			}
		}
		if err != nil {
			sugarLogger.Errorf("get file: %s error: %v\n", filename, err.Error())
			// TDFSLogger.Printf("get file:%v error=%v\n", filename, err.Error())
			// fmt.Printf("get file=%v error=%v\n", filename, err.Error())
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
			sugarLogger.Errorf("namenode put json to byte error: %s", err)
			// fmt.Println("namenode put json to byte error", err)
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
			sugarLogger.Errorf("namenode put json to byte error: %s", err)
			// fmt.Println("namenode put json to byte error", err)
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
			sugarLogger.Errorf("namenode put json to byte error: %s", err)
			// fmt.Println("namenode put json to byte error", err)
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
			sugarLogger.Errorf("namenode put json to byte error: %s", err)
			// fmt.Println("namenode put json to byte error", err)
		}
		res := namenode.NameSpace.CreateFolder(dataMap["curPath"], dataMap["folderName"])
		context.JSON(http.StatusOK, []bool{res})
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

func (namenode *NameNode) AllocateChunk() (rlList [REDUNDANCE]ReplicaLocation, tempDNArr []int) {
	redundance := namenode.REDUNDANCE
	var max [REDUNDANCE]int
	// var tempDNArr [REDUNDANCE]int
	// var tempDNArr [REDUNDANCE]int
	// tempDNArr = make([]int, REDUNDANCE)
	//必须保证同一个chunk及其备份不能在同一个DN里面
	//用mapset来保证DN的唯一性
	// set := mapset.NewSet()							
	for i := 0; i < redundance; i++ {
		max[i] = 0
		//找到目前空闲块最多的NA
		for j := 0; j < namenode.DNNumber; j++ {
			//遍历每一个DN，找到空闲块最多的前redundance个DN
			if namenode.DataNodes[j].StorageAvail > namenode.DataNodes[max[i]].StorageAvail {
				max[i] = j
			}
		}
		tempDNArr = append(tempDNArr, max[i])
		//将该位置存下来，后面发送文件时需要用来更新DN地址
		rlList[i].index = max[i]
		//ServerLocation是DN地址
		rlList[i].ServerLocation = namenode.DataNodes[max[i]].Location
		//ReplicaNum是DN已用的块
		rlList[i].ReplicaNum = namenode.DataNodes[max[i]].ChunkAvail[0]
		n := namenode.DataNodes[max[i]].StorageAvail

		namenode.DataNodes[max[i]].ChunkAvail[0] = namenode.DataNodes[max[i]].ChunkAvail[n-1]
		namenode.DataNodes[max[i]].ChunkAvail = namenode.DataNodes[max[i]].ChunkAvail[0 : n-1]
		namenode.DataNodes[max[i]].StorageAvail--
	}
	//对REDUNDANCE个DataNode分别记录chunk备份的映射

	return rlList, tempDNArr
}

func (namenode *NameNode) SetConfig(location string, dnnumber int, redundance int, dnlocations []string) {
	namenode.OldToNewMap = make(map[string]string)
	temp := strings.Split(location, ":")
	res, err := strconv.Atoi(temp[2])
	if err != nil {
		sugarLogger.Errorf("namenode error at atoi parse port: %s", err)
		// fmt.Println("XXX NameNode error at Atoi parse Port", err.Error())
		// TDFSLogger.Fatal("XXX NameNode error: ", err)
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
			sugarLogger.Errorf("namenode error at get meta of %s: %s", namenode.DNLocations[i], err)
			// fmt.Println("XXX NameNode error at Get meta of ", namenode.DNLocations[i], ": ", err.Error())
			// TDFSLogger.Fatal("XXX NameNode error: ", err)
		}
		defer response.Body.Close()

		var dn DataNode
		err = json.NewDecoder(response.Body).Decode(&dn)
		if err != nil {
			sugarLogger.Errorf("namenode error at decode response to json: %s", err)
			// fmt.Println("XXX NameNode error at decode response to json.", err.Error())
			// TDFSLogger.Fatal("XXX NameNode error: ", err)
		}
		// fmt.Println(dn)
		// err = json.Unmarshal([]byte(str), &dn)
		dn.LastQuery = time.Now().Unix()
		namenode.DataNodes = append(namenode.DataNodes, dn)
	}
	namenode.ShowInfo()
	go namenode.MonitorDN()
}

func (namenode *NameNode) StartNewDataNode(c []string){
	var attr = os.ProcAttr{
		Dir: "../dn",
		Env: os.Environ(),
		Files: []*os.File{
			os.Stdin,
			nil,
			nil,
		},
		// Sys: sysproc,
	}

	process, err := os.StartProcess(c[0], c, &attr)
	if err == nil {
		// It is not clear from docs, but Realease actually detaches the process
		err = process.Release()
		if err != nil {
			fmt.Println(err.Error())
		}

	} else {
		fmt.Println(err.Error())
	}

}
