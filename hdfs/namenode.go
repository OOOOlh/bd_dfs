package hdfs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"

	"go.uber.org/zap"

	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var mu sync.Locker

func (namenode *NameNode) MonitorDN() {
	defer func() {
		if x := recover(); x != nil {
			sugarLogger.Errorf("panic when monitor DataNode, err: %v\n", x)
		}
	}()

	ticker := time.NewTicker(5 * time.Second)
	for {
		<-ticker.C
		for i := 0; i < len(namenode.DataNodes); i++ {
			t := time.Now().Unix() - namenode.DataNodes[i].LastQuery
			//如果大于30s，就表示该DN出现问题，无法完成上报任务。新建一个节点，将所有数据复制到新节点上
			flag := false
			if t > 30 && flag == false {
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
				//替换掉旧的节点
				namenode.Map[newLocation] = i

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

				for i := 0; i < (dataNode.StorageTotal - dataNode.StorageAvail); i++ {
					for j := 0; j < REDUNDANCE-1; j++ {
						copyChunkLocation := dataNode.ChunkCopy[i][j].ServerLocation
						copyChunkReplicaNum := dataNode.ChunkCopy[i][j].ReplicaNum

						if d, ok := namenode.OldToNewMap[copyChunkLocation]; ok {
							copyChunkLocation = d
						}

						//存储新的DN位置，和备份DN的chunk位置
						//将该信息发送给副本DN，让副本DN给新DN发送对应的chunk
						rm := &ReplicaLocation{
							ServerLocation: newLocation,
							ReplicaNum:     copyChunkReplicaNum,
							OldNum:         i,
						}

						d, err := json.Marshal(rm)
						if err != nil {
							fmt.Println("json to byte[] error", err)
						}

						reader := bytes.NewReader(d)
						resp, err := http.Post(copyChunkLocation+"/fixchunk", "application/json", reader)
						if err != nil {
							fmt.Println("http post error", err)
						}

						_, err = ioutil.ReadAll(resp.Body)
						if err != nil {
							fmt.Println("NameNode error at Read response", err.Error())
							sugarLogger.Errorf("read response error: %s", err)
						}
					}
				}
				namenode.DataNodes[i] = dn

				namenode.StandByDataNode = namenode.StandByDataNode[1:]
				namenode.Mu.Unlock()
			}
			flag = false
		}
	}
}

func (namenode *NameNode) Run() {
	router := gin.Default()
	router.Use(MwPrometheusHttp)
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	router.GET("/leader", func(c *gin.Context) {
		c.String(http.StatusOK, namenode.LeaderLocation)
	})

	router.POST("/nn_heartbeat", func(c *gin.Context) {
		b, _ := c.GetRawData()
		if len(b) == 0 {
			fmt.Println("empty data")
			c.JSON(http.StatusBadRequest, nil)
			return
		}
		heartBeat := &NNHeartBeat{}
		if err := json.Unmarshal(b, heartBeat); err != nil {
			fmt.Println("unmarshal heartbeat data error")
			c.JSON(http.StatusBadRequest, nil)
			return
		}
		fmt.Printf("receive heartbeat=%+v\n", heartBeat)
		fmt.Printf("editLog=%+v\n", heartBeat.EditLog)
		if heartBeat.Term < namenode.Term {
			fmt.Println("term is too low")
			c.JSON(http.StatusBadRequest, nil)
			return
		} else if heartBeat.Term > namenode.Term {
			fmt.Println("bigger term")
			namenode.Term = heartBeat.Term
			namenode.IsLeader = false
			namenode.LeaderLocation = heartBeat.LeaderLocation
		}
		if !namenode.IsLeader {
			fmt.Println("follower reset ticker")
			namenode.HeartBeatTicker.Reset(3 * HeartBeatInterval)
			namenode.LeaderLocation = heartBeat.LeaderLocation
			namenode.Term = heartBeat.Term
		}
		if len(heartBeat.EditLog) == 0 {
			// 不带editlog的心跳同步
			fmt.Println("without edit log")
			fmt.Printf("leader commit=%+v follower commit=%+v\n", heartBeat.LeaderCommitIndex, namenode.CommitIndex)
			if heartBeat.LeaderCommitIndex == namenode.CommitIndex {
				c.JSON(http.StatusOK, namenode.CommitIndex)
				return
			}
			if heartBeat.LeaderCommitIndex > namenode.CommitIndex {
				for _, log := range namenode.TmpLog[namenode.CommitIndex:] {
					// 应用变动到namenode文件树
					namenode.ApplyEditLog(log)
					fmt.Println("change namenode file tree")
					namenode.CommitIndex = log.CommitIndex
				}
				c.JSON(http.StatusOK, namenode.CommitIndex)
				return
			}
			c.JSON(http.StatusBadRequest, namenode.CommitIndex)
			return
		} else {
			fmt.Println("has edit log")
			fmt.Printf("pre log index=%+v tmpLog=%+v\n", heartBeat.PreLogIndex, namenode.TmpLog)
			if heartBeat.PreLogIndex > len(namenode.TmpLog) {
				fmt.Println("bigger pre log index")
				c.JSON(http.StatusNotAcceptable, namenode.CommitIndex)
				return
			}
			if heartBeat.PreLogIndex > 0 && heartBeat.PreLogTerm != namenode.TmpLog[heartBeat.PreLogIndex-1].Term {
				c.JSON(http.StatusBadRequest, namenode.CommitIndex)
				return
			}
			for _, log := range heartBeat.EditLog {
				if log.CommitIndex > len(namenode.TmpLog) {
					namenode.TmpLog = append(namenode.TmpLog, log)
				} else {
					if namenode.TmpLog[log.CommitIndex-1].Term != log.Term ||
						namenode.TmpLog[log.CommitIndex-1].CommitIndex != log.CommitIndex {
						namenode.TmpLog[log.CommitIndex-1] = log
					}
				}
				if log.CommitIndex <= heartBeat.LeaderCommitIndex &&
					log.CommitIndex > namenode.CommitIndex {
					// 应用到文件树
					namenode.ApplyEditLog(log)
					fmt.Println("change namenode file tree")
					namenode.CommitIndex = log.CommitIndex
				}
			}
			logStr, _ := json.Marshal(namenode.TmpLog)
			fmt.Printf("tmpLog=%+v commitIndex=%+v\n", string(logStr), namenode.CommitIndex)
			c.JSON(http.StatusOK, namenode.CommitIndex)
			return
		}
	})

	router.POST("/vote", func(c *gin.Context) {
		b, _ := c.GetRawData()
		vote := &Vote{}
		if len(b) == 0 {
			fmt.Println("put request body为空")
		}
		if err := json.Unmarshal(b, vote); err != nil {
			fmt.Println("namenode put json to byte error", err)
		}
		fmt.Printf("receive vote=%+v\n time=%+v", vote, time.Now())
		if vote.Term <= namenode.Term {
			c.JSON(http.StatusBadRequest, nil)
			return
		}
		if vote.LeaderCommitIndex < namenode.CommitIndex {
			c.JSON(http.StatusBadRequest, nil)
			return
		}
		namenode.Term = vote.Term
		c.JSON(http.StatusOK, nil)
	})

	router.POST("/heartbeat", func(c *gin.Context) {
		d, _ := c.GetRawData()
		datanode := DataNode{}
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

		if len(datanode.ChunkAvail) != len(localDataNode.ChunkAvail) {
			sugarLogger.Errorf("datanode %s : 可用chunk数目出错\n", datanode.Location)
		}
		c.String(http.StatusOK, "")
	})

	router.POST("/put", func(c *gin.Context) {
		b, _ := c.GetRawData()
		file := &File{}
		if len(b) == 0 {
			sugarLogger.Warn("client to namenode put request body为空")
		}
		if err := json.Unmarshal(b, file); err != nil {
			sugarLogger.Errorf("namenode put json to byte error: %s", err)
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

			for j := 0; j < len(replicaLocationList); j++ {
				index := replicaLocationList[j].index
				t := 0
				for k := 0; ; k++ {
					if k == len(replicaLocationList) {
						break
					} else if k == j {
						continue
					}
					namenode.DataNodes[index].ChunkCopy[replicaLocationList[j].ReplicaNum][t] = replicaLocationList[k]
					t = t + 1
				}

			}
			fileChunk := &FileChunk{}
			file.Chunks = append(file.Chunks, *fileChunk)
			file.Chunks[i].ReplicaLocationList = replicaLocationList
		}
		// 复制日志
		success := namenode.AddEditLog("put", file.RemotePath+file.Name, file, false, nil, nil)
		if !success {
			// 同步不成功，回滚namenode块信息
			for _, chunk := range file.Chunks {
				for _, r := range chunk.ReplicaLocationList {
					for _, dn := range namenode.DataNodes {
						if dn.Location == r.ServerLocation {
							dn.ChunkAvail = append(dn.ChunkAvail, r.ReplicaNum)
							dn.StorageAvail++
							break
						}
					}
				}
			}
			c.JSON(http.StatusBadRequest, nil)
			return
		}
		fmt.Printf("add log success=%v\n", success)
		namenode.PutFile(file)
		c.JSON(http.StatusOK, file)
	})
	//
	router.GET("/getfile", func(c *gin.Context) {
		filename := c.Query("filename")
		fmt.Println("$ getfile ...", filename)
		node := namenode.NameSpace
		file, err := node.GetFileNode(filename)
		sugarLogger.Info("获取到的文件为:", file)
		if err != nil {
			fmt.Println(err)
		}

		if file == nil{
			file = &File{}
		}

		for i := 0; i < len(file.Chunks); i++ {
			for j := 0; j < len(file.Chunks[i].ReplicaLocationList); j++ {
				file.Chunks[i].ReplicaLocationList[j].ServerLocation = namenode.DataNodes[file.Chunks[i].ReplicaLocationList[j].index].Location
			}
		}
		if err != nil {
			sugarLogger.Errorf("get file: %s error: %v\n", filename, err.Error())
			c.JSON(http.StatusNotFound, err.Error())
			return
		}
		c.JSON(http.StatusOK, file)
	})

	router.POST("/delfile", func(context *gin.Context) {
		b, _ := context.GetRawData()
		var dataMap map[string]string
		if err := json.Unmarshal(b, &dataMap); err != nil {
			sugarLogger.Errorf("namenode put json to byte error: %s", err)
		}
		fmt.Println("删除测试")

		filename := dataMap["filename"]
		sugarLogger.Info("$ delfile ...", filename)

		node := namenode.NameSpace
		file, err := node.GetFileNode(filename)
		if err != nil {
			sugarLogger.Errorf("delete file: %s error: %v\n", filename, err.Error())
			context.JSON(http.StatusNotFound, err.Error())
			return
		}

		// 复制日志
		success := namenode.AddEditLog("delfile", "", file, false, nil, nil)
		if !success {
			context.JSON(http.StatusBadRequest, nil)
			return
		}

		for i := 0; i < len(file.Chunks); i++ {
			namenode.DelChunk(*file, i)
		}
		*file = File{}
		context.JSON(http.StatusOK, file)
	})

	router.POST("/reFolderName", func(context *gin.Context) {
		b, _ := context.GetRawData()
		var dataMap map[string]string
		if err := json.Unmarshal(b, &dataMap); err != nil {
			sugarLogger.Errorf("namenode put json to byte error: %s", err)

		}
		// 复制日志
		success := namenode.AddEditLog("reFolderName", "", nil, false, dataMap, nil)
		if !success {
			context.JSON(http.StatusBadRequest, nil)
			return
		}
		res := namenode.NameSpace.ReNameFolderName(dataMap["preFolder"], dataMap["reNameFolder"])
		context.JSON(http.StatusOK, res)
	})

	router.POST("/getFolders", func(context *gin.Context) {
		b, _ := context.GetRawData()
		var dataMap map[string]string
		if err := json.Unmarshal(b, &dataMap); err != nil {
			sugarLogger.Errorf("namenode put json to byte error: %s", err)
		}
		_, folders := namenode.NameSpace.GetFileList(dataMap["fname"])
		var filenames []string
		for i := 0; i < len(folders); i++ {
			filenames = append(filenames, folders[i].Name)
		}
		context.JSON(http.StatusOK, filenames)
	})

	router.POST("/getFiles", func(context *gin.Context) {
		b, _ := context.GetRawData()
		var dataMap map[string]string
		if err := json.Unmarshal(b, &dataMap); err != nil {
			sugarLogger.Errorf("namenode put json to byte error: %s", err)
		}
		files, _ := namenode.NameSpace.GetFileList(dataMap["fname"])
		var filenames []string
		for i := 0; i < len(files); i++ {
			filenames = append(filenames, files[i].Name)
		}
		context.JSON(http.StatusOK, filenames)
	})

	//创建文件目录
	router.POST("/mkdir", func(context *gin.Context) {
		b, _ := context.GetRawData()
		var dataMap map[string]string
		if err := json.Unmarshal(b, &dataMap); err != nil {
			sugarLogger.Errorf("namenode put json to byte error: %s", err)
		}
		// 复制日志
		success := namenode.AddEditLog("mkdir", "", nil, false, dataMap, nil)
		if !success {
			context.JSON(http.StatusBadRequest, nil)
			return
		}
		res := namenode.NameSpace.CreateFolder(dataMap["curPath"], dataMap["folderName"])
		context.JSON(http.StatusOK, []bool{res})
	})

	//获取当前所有文件对应的位置信息, 节点扩容
	router.GET("/getFilesChunkLocation", func(context *gin.Context) {
		fileClunksLocation := namenode.NameSpace.GetFilesChunkLocation()
		context.JSON(http.StatusOK, fileClunksLocation)
	})

	// 节点扩容之后更新NameNode
	router.POST("/updataNewNode", func(context *gin.Context) {
		b, _ := context.GetRawData() // 从c.Request.Body读取请求数据
		var dataMap map[string][]string
		if err := json.Unmarshal(b, &dataMap); err != nil {
			sugarLogger.Errorf("namenode put json to byte error: %s", err)
		}
		fmt.Printf("newNode dir %s \n", dataMap["newNode"][0])
		fmt.Printf("newNode port %s \n", dataMap["newNode"][1])
		// 复制日志
		success := namenode.AddEditLog("updataNewNode", "", nil, false, nil, dataMap)
		if !success {
			context.JSON(http.StatusBadRequest, nil)
			return
		}

		namenode.UpdateNewNode(dataMap)
		context.JSON(http.StatusOK, "update success!")
	})

	router.POST("/getfilestat", func(c *gin.Context) {
		b, _ := c.GetRawData()
		var dataMap map[string]string
		if err := json.Unmarshal(b, &dataMap); err != nil {
			fmt.Println("namenode put json to byte error", err)
		}
		fmt.Println("there:")
		fmt.Println(dataMap["fname"])
		file, err := namenode.NameSpace.GetFileNode(dataMap["fname"])
		if err != nil {
			fmt.Printf("get file stat=%v error=%v\n", dataMap["fname"], err.Error())
			c.JSON(http.StatusNotFound, err.Error())
		}
		filename := file.Name
		length := file.Length
		fmt.Printf("filename: %s, filelength: %d\n", filename, length)
		c.JSON(http.StatusOK, file)
	})
	router.Run(":" + strconv.Itoa(namenode.Port))
}

func (namenode *NameNode) DelChunk(file File, num int) {
	//预删除文件的块信息
	for i := 0; i < REDUNDANCE; i++ {
		chunklocation := file.Chunks[num].ReplicaLocationList[i].ServerLocation
		chunknum := file.Chunks[num].ReplicaLocationList[i].ReplicaNum
		index := namenode.Map[chunklocation]
		namenode.DataNodes[index].ChunkAvail = append(namenode.DataNodes[index].ChunkAvail, chunknum)
		namenode.DataNodes[index].StorageAvail++
	}
}

func (namenode *NameNode) AllocateChunk() (rlList [REDUNDANCE]ReplicaLocation, tempDNArr []int) {
	redundance := namenode.REDUNDANCE
	var max [REDUNDANCE]int
	for i := 0; i < redundance; i++ {
		max[i] = 0
		for j := 0; j < namenode.DNNumber; j++ {
			if namenode.DataNodes[j].StorageAvail > namenode.DataNodes[max[i]].StorageAvail {
				max[i] = j
			}
		}
		tempDNArr = append(tempDNArr, max[i])

		rlList[i].index = max[i]
		rlList[i].ServerLocation = namenode.DataNodes[max[i]].Location
		rlList[i].ReplicaNum = namenode.DataNodes[max[i]].ChunkAvail[0]
		n := namenode.DataNodes[max[i]].StorageAvail

		namenode.DataNodes[max[i]].ChunkAvail[0] = namenode.DataNodes[max[i]].ChunkAvail[n-1]
		namenode.DataNodes[max[i]].ChunkAvail = namenode.DataNodes[max[i]].ChunkAvail[0 : n-1]
		namenode.DataNodes[max[i]].StorageAvail--
	}
	return rlList, tempDNArr
}

func (namenode *NameNode) SetConfig(location string, dnnumber int, redundance int, dnlocations []string, nnlocations []string) {
	namenode.OldToNewMap = make(map[string]string)
	temp := strings.Split(location, ":")
	res, err := strconv.Atoi(temp[2])
	if err != nil {
		sugarLogger.Errorf("namenode error at atoi parse port: %s", err)
	}
	namenode.NameSpace = &Folder{
		Name:   "root",
		Folder: make([]*Folder, 0),
		Files:  make([]*File, 0),
	}
	namenode.Port = res
	namenode.Location = location
	namenode.NNLocations = nnlocations
	namenode.DNNumber = dnnumber
	namenode.DNLocations = dnlocations
	namenode.REDUNDANCE = redundance
	namenode.TmpLog = make([]*EditLog, 0)
	namenode.MatchIndex = make(map[string]int)
	namenode.HeartBeatTicker = time.NewTicker(HeartBeatInterval)
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

func (namenode *NameNode) GetDNMeta() {
	namenode.Map = make(map[string]int)
	for i := 0; i < len(namenode.DNLocations); i++ {
		namenode.Map[namenode.DNLocations[i]] = i
		response, err := http.Get(namenode.DNLocations[i] + "/getmeta")
		if err != nil {
			sugarLogger.Errorf("namenode error at get meta of %s: %s", namenode.DNLocations[i], err)
		}
		defer response.Body.Close()

		var dn DataNode
		err = json.NewDecoder(response.Body).Decode(&dn)
		if err != nil {
			sugarLogger.Errorf("namenode error at decode response to json: %s", err)
		}
		dn.LastQuery = time.Now().Unix()
		namenode.DataNodes = append(namenode.DataNodes, dn)
	}
	namenode.ShowInfo()
	go namenode.MonitorDN()
}

func (namenode *NameNode) StartNewDataNode(c []string) {
	var attr = os.ProcAttr{
		Dir: "../dn",
		Env: os.Environ(),
		Files: []*os.File{
			os.Stdin,
			nil,
			nil,
		},
	}
	process, err := os.StartProcess(c[0], c, &attr)
	if err == nil {
		err = process.Release()
		if err != nil {
			fmt.Println(err.Error())
		}

	} else {
		fmt.Println(err.Error())
	}
}

func (namenode *NameNode) PutFile(file *File) *File {
	path := strings.Split(file.RemotePath, "/")

	var n *Folder
	ff := namenode.NameSpace
	folder := &ff.Folder
	if len(path) == 2 || (len(path) == 3 && path[2] == "") {
		n = ff
	} else {
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
				var tempFloder Folder = Folder{}
				tempFloder.Name = p
				*folder = append(*folder, &tempFloder)
				folder = &(*folder)[len(*folder)-1].Folder
				n = &tempFloder
			} else {
				folder = &n.Folder
			}
		}
	}

	var exist bool
	var changed bool = true
	var f *File
	for _, f = range n.Files {
		exist = false

		if f.Name == file.Name {
			exist = true

			if f.Info == file.Info {
				sugarLogger.Info("namenode: file exists and not changed")
				changed = false
			}
			break
		}
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
	return file
}

func (namenode *NameNode) UpdateNewNode(dataMap map[string][]string) {
	namenode.DNLocations = append(namenode.DNLocations, "http://localhost:"+dataMap["newNode"][1])
	port, _ := strconv.Atoi(dataMap["newNode"][1])
	chunkAvail := []int{}
	for i := len(dataMap["filePath"]); i < 400; i++ {
		chunkAvail = append(chunkAvail, i)
	}
	newNode := DataNode{
		Location:     "http://localhost:" + dataMap["newNode"][1],
		Port:         port,
		StorageTotal: 400,
		StorageAvail: 400 - len(dataMap["filePath"]),
		ChunkAvail:   chunkAvail,
		LastEdit:     0,
		DATANODE_DIR: "nil",
		NNLocation:   []string{},
		LastQuery:    0,
		ZapLogger:    &zap.SugaredLogger{},
	}
	namenode.DataNodes = append(namenode.DataNodes, newNode)
}
