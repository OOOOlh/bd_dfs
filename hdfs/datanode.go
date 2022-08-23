package hdfs

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func (datanode *DataNode) Run() {

	router := gin.Default()
	router.Use(MwPrometheusHttp)
	// register the `/metrics` route.
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	router.POST("/putchunk", func(c *gin.Context) {
		// c.Request.ParseMultipartForm(32 << 20) //上传最大文件限制32M
		// chunkNum := c.Request.Form.Get("chunkNum") //通过这种方式在gin中也可以读取到POST的参数，ginb
		ReplicaNum := c.PostForm("ReplicaNum")
		datanode.DNLogger.Printf("* ReplicaNum= %s\n", ReplicaNum)
		file, _, err := c.Request.FormFile("putchunk")
		if err != nil {
			c.String(http.StatusBadRequest, "XXX Bad request")
			datanode.DNLogger.Fatalf("%s DataNode error: %v\n", datanode.Location, err)
			return
		}
		// filename := header.Filename
		// fmt.Println("****************************************")
		// fmt.Println(file, err, filename)
		// fmt.Println("****************************************")

		//ReplicaNum是下一个将要被使用的chunk
		chunkout, err := os.Create(datanode.DATANODE_DIR + "/chunk/chunk-" + ReplicaNum) //在服务器本地新建文件进行存储
		if err != nil {
			fmt.Println("XXX DataNode error at Create chunk file", err.Error())
			datanode.DNLogger.Fatalf("%s DataNode error at Create chunk file:%v\n", datanode.Location, err.Error())
		}
		defer chunkout.Close()
		io.Copy(chunkout, file) //在服务器本地新建文件进行存储

		chunkdata := readFileByBytes(datanode.DATANODE_DIR + "/chunk/chunk-" + ReplicaNum)

		hash := sha256.New()
		// if _, err := io.Copy(hash, file); err != nil {fmt.Println("DataNode error at sha256", err.Error())}
		hash.Write(chunkdata)
		hashStr := hex.EncodeToString(hash.Sum(nil))
		// fmt.Println("** chunk hash", ReplicaNum, ": %s", hashStr)
		datanode.DNLogger.Printf("chunk hash %s, hashStr %s\n", ReplicaNum, hashStr)
		FastWrite(datanode.DATANODE_DIR+"/achunkhashs/chunkhash-"+ReplicaNum, []byte(hashStr))

		//100
		n := datanode.StorageAvail
		datanode.ChunkAvail[0] = datanode.ChunkAvail[n-1]
		datanode.ChunkAvail = datanode.ChunkAvail[0 : n-1]
		datanode.StorageAvail--

		c.String(http.StatusCreated, "PutChunk SUCCESS\n")
	})

	router.GET("/getchunk/:chunknum", func(c *gin.Context) {
		chunknum := c.Param("chunknum")
		num, err := strconv.Atoi(chunknum)
		if err != nil {
			// fmt.Println("XXX DataNode error(getchunk) at Atoi parse chunknum to int", err.Error())
			datanode.DNLogger.Fatalf("%s DataNode error(getchunk) at Atoi parse chunknum to int: %v\n", datanode.Location, err.Error())
		}

		fdata := readFileByBytes(datanode.DATANODE_DIR + "/chunk/chunk-" + strconv.Itoa(num))
		c.String(http.StatusOK, string(fdata))
	})

	router.GET("/getchunkhash/:chunknum", func(c *gin.Context) {
		chunknum := c.Param("chunknum")
		num, err := strconv.Atoi(chunknum)
		if err != nil {
			// fmt.Println("XXX DataNode error(getchunkhash) at Atoi parse chunknum to int", err.Error())
			datanode.DNLogger.Fatalf("%s DataNode error(getchunkhash) at Atoi parse chunknum to int:%v\n", datanode.Location, err.Error())
		}
		// fmt.Println("Parsed num: ", num)

		fdata := readFileByBytes(datanode.DATANODE_DIR + "/achunkhashs/chunkhash-" + strconv.Itoa(num))
		c.String(http.StatusOK, string(fdata))
	})

	router.DELETE("/delchunk/:chunknum", func(c *gin.Context) {
		chunknum := c.Param("chunknum")
		num, err := strconv.Atoi(chunknum)
		if err != nil {
			// fmt.Println("XXX DataNode error at Atoi parse chunknum to int", err.Error())
			datanode.DNLogger.Fatalf("%s DataNode error at Atoi parse chunknum to int: %v\n", datanode.Location, err.Error())
		}
		// fmt.Println("Parsed num: ", num)

		CleanFile(datanode.DATANODE_DIR + "/chunk/chunk-" + strconv.Itoa(num))
		CleanFile(datanode.DATANODE_DIR + "/achunkhashs/chunkhash-" + strconv.Itoa(num))

		c.String(http.StatusOK, "delete DataNode{*}/chunk/chunk-"+strconv.Itoa(num)+" SUCCESS")
	})

	// router.GET("/delchunk/:chunknum", func(c *gin.Context) {
	// 	chunknum := c.Param("chunknum")
	// 	num, err := strconv.Atoi(chunknum)
	// 	if err!=nil{
	// 		fmt.Println("XXX DataNode error at Atoi parse chunknum to int", err.Error())
	// 		TDFSLogger.Fatal("XXX DataNode error: ", err)
	// 	}
	// 	fmt.Println("Parsed num: ", num)

	// 	CleanFile(datanode.DATANODE_DIR+"/chunk-"+strconv.Itoa(num))
	// 	// CleanFile(datanode.DATANODE_DIR+"/achunkhashs/chunkhash-"+strconv.Itoa(num))
	// 	DeleteFile(datanode.DATANODE_DIR+"/achunkhashs/chunkhash-"+strconv.Itoa(num))

	// 	c.String(http.StatusOK, "delete DataNode{*}/chunk-"+strconv.Itoa(num)+" SUCCESS")
	// })

	// router.POST("/putmeta", func(c *gin.Context) {
	// 	ReplicaNum := c.PostForm("ReplicaNum")
	// 	fmt.Printf("*** New DataNode Data = %s\n",ReplicaNum)
	// })

	router.GET("/getmeta", func(c *gin.Context) {
		c.JSON(http.StatusOK, datanode)
	})

	router.Run(":" + strconv.Itoa(datanode.Port))
}

//心跳上报
func (datanode *DataNode) SendHeartbeat(){
	defer func () {
		if x := recover(); x != nil{
			datanode.DNLogger.Fatalf("panic when DataNode %s send heartbeat to namenode, err: %v\n", datanode.Location, x) 
		}
	}()

	//每1min上报一次
	datanode.Ticker = time.NewTicker(30 * time.Second)

	for{
		<- datanode.Ticker.C
		go func ()  {

			d, err := json.Marshal(datanode)
			if err != nil {
				datanode.DNLogger.Fatalf("%s Datanode send heartbeat json to byte[] error: %v\n", datanode.Location, err.Error())
			}
			// 序列化
			reader := bytes.NewReader(d)
			_, err = http.Post(datanode.NNLocation[0]+"/heartbeat", "application/json", reader)
			if err != nil {
				// fmt.Println("http post error", err)
				datanode.DNLogger.Fatalf("%s Datanode send heartbeat http post error: %v\n", datanode.Location, err.Error())
			}
		}()

	}
}

func (datanode *DataNode) SetConfig(port string) {

	//配置DN日志
	logFile := OpenFile(datanode.DATANODE_DIR + "/DNLog.txt")
	datanode.DNLogger = log.New(logFile, "Log " + port + ":", log.Ldate|log.Ltime|log.Lshortfile)

	//所有NN地址
	dnlocations := []string{"http://localhost:11090"}

	res, err := strconv.Atoi(port)
	if err != nil {
		// fmt.Println("XXX DataNode error at Atoi parse Port", err.Error())
		datanode.DNLogger.Fatalf("%s DataNode error at Atoi parse Port:%v\n", port, err)
	}
	datanode.Port = res
	datanode.Location = "http://localhost:" + port
	datanode.StorageTotal = DN_CAPACITY
	datanode.StorageAvail = datanode.StorageTotal
	datanode.NNLocation = dnlocations

	datanode.ChunkAvail = append(datanode.ChunkAvail, 0)
	for i := 1; i < datanode.StorageAvail; i++ {
		datanode.ChunkAvail = append(datanode.ChunkAvail, datanode.StorageTotal-i)
	}

	datanode.LastEdit = time.Now().Unix()
	for num := 0; num < datanode.StorageTotal; num++ {
		CreateFile(datanode.DATANODE_DIR + "/chunk/chunk-" + strconv.Itoa(num))
	}
	// fmt.Println("************************************************************")
	// fmt.Println("************************************************************")
	// fmt.Printf("*** Successfully Set Config data for a datanode\n")
	// datanode.ShowInfo()
	// fmt.Println("************************************************************")
	// fmt.Println("************************************************************")

	datanode.DNLogger.Println("************************************************************")
	datanode.DNLogger.Println("************************************************************")
	datanode.DNLogger.Println("*** Successfully Set Config data for a datanode")
	datanode.ShowInfo()
	datanode.DNLogger.Println("************************************************************")
	datanode.DNLogger.Println("************************************************************")
}

//目前是datanode断电就删除原来的数据
func (datanode *DataNode) Reset() {
	datanode.reset(datanode.DATANODE_DIR + "/chunk")
	datanode.reset(datanode.DATANODE_DIR + "/achunkhashs")
	// for i := 0; i < datanode.StorageTotal; i++ {
	// 	CleanFile(datanode.DATANODE_DIR + "/chunk-" + strconv.Itoa(i))
	// }
}

func (datanode *DataNode)reset(dir string){
	exist, err := PathExists(dir)
	if err != nil {
		// fmt.Println("XXX DataNode error at Get Dir chunkhashs", err.Error())
		datanode.DNLogger.Fatalf("%s DataNode error at Get Dir: %v\n", datanode.Location, err)
	}

	if !exist {
		// 不存在创建chunkhash
		err = os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			// fmt.Println("XXX DataNode error at MkdirAll chunkhashs", err.Error())
			datanode.DNLogger.Fatalf("%s DataNode error at MkdirAll: %v \n", datanode.Location, err)
		}
	} else {
		// 存在首先删除然后创建chunkhash
		err := os.RemoveAll(dir)
		if err != nil {
			// fmt.Println("XXX DataNode error at RemoveAll file hash data", err.Error())
			datanode.DNLogger.Fatalf("%s DataNode error at RemoveAll file hash data: %v \n", datanode.Location, err)
		}

		err = os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			// fmt.Println("XXX DataNode error at MkdirAll chunkhashs", err.Error())
			datanode.DNLogger.Fatalf("%s DataNode error at MkdirAll: %v \n", datanode.Location, err)
		}
	}
}

func (datanode *DataNode) ShowInfo() {
	datanode.DNLogger.Printf("Location: %s\n", datanode.Location)
	datanode.DNLogger.Printf("DATANODE_DIR: %s\n", datanode.DATANODE_DIR)
	datanode.DNLogger.Printf("Port: %d\n", datanode.Port)
	datanode.DNLogger.Printf("StorageTotal: %d\n", datanode.StorageTotal)
	datanode.DNLogger.Printf("StorageAvail: %d\n", datanode.StorageAvail)
	datanode.DNLogger.Printf("ChunkAvail: %d\n", datanode.ChunkAvail)
	datanode.DNLogger.Printf("LastEdit: %d\n", datanode.LastEdit)
}
