package hdfs

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
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
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	router.POST("/putChunkBybytes", func(context *gin.Context) {
		b, _ := context.GetRawData()
		var dataMap map[string][]byte
		if err := json.Unmarshal(b, &dataMap); err != nil {
			sugarLogger.Errorf("namenode put json to byte error: %s", err)
		}
		chunkId := string(dataMap["chunkId"])
		data := dataMap["data"]
		chunkout, err := os.Create(datanode.DATANODE_DIR + "/chunk/chunk-" + chunkId)
		if err != nil {
			fmt.Println("create file success!")
		}
		chunkout.Write(data)
		defer chunkout.Close()
		context.String(http.StatusOK, "putChunkBybytes SUCCESS!")
	})
	
	router.POST("/putchunk", func(c *gin.Context) {
		ReplicaNum := c.PostForm("ReplicaNum")
		datanode.ZapLogger.Infof("* ReplicaNum=%s", ReplicaNum)
		file, _, err := c.Request.FormFile("putchunk")
		if err != nil {
			c.String(http.StatusBadRequest, "XXX Bad request")
			datanode.ZapLogger.Fatalf("%s DataNode error: %v", datanode.Location, err)
			return
		}

		chunkout, err := os.Create(datanode.DATANODE_DIR + "/chunk/chunk-" + ReplicaNum)
		if err != nil {
			fmt.Println("XXX DataNode error at Create chunk file", err.Error())
			datanode.ZapLogger.Fatalf("%s DataNode error at Create chunk file:%v", datanode.Location, err.Error())
		}
		defer chunkout.Close()
		io.Copy(chunkout, file)

		chunkdata := readFileByBytes(datanode.DATANODE_DIR + "/chunk/chunk-" + ReplicaNum)

		hash := sha256.New()
		hash.Write(chunkdata)
		hashStr := hex.EncodeToString(hash.Sum(nil))
		datanode.ZapLogger.Infof("chunk hash %s, hashStr %s", ReplicaNum, hashStr)
		FastWrite(datanode.DATANODE_DIR+"/achunkhashs/chunkhash-"+ReplicaNum, []byte(hashStr))

		n := datanode.StorageAvail
		datanode.ChunkAvail[0] = datanode.ChunkAvail[n-1]
		datanode.ChunkAvail = datanode.ChunkAvail[0 : n-1]
		datanode.StorageAvail--

		fmt.Printf("每个chunk大小:%d Byte, 剩余可用chunk:%d, 总chunk:%d\n", SPLIT_UNIT, datanode.StorageAvail, datanode.StorageTotal)

		c.String(http.StatusCreated, "PutChunk SUCCESS\n")
	})

	router.GET("/getchunk/:chunknum", func(c *gin.Context) {
		chunknum := c.Param("chunknum")
		num, err := strconv.Atoi(chunknum)
		if err != nil {
			datanode.ZapLogger.Errorf("%s DataNode error(getchunk) at Atoi parse chunknum to int: %v", datanode.Location, err.Error())
		}

		fdata := readFileByBytes(datanode.DATANODE_DIR + "/chunk/chunk-" + strconv.Itoa(num))
		c.String(http.StatusOK, string(fdata))
	})

	router.GET("/getchunkhash/:chunknum", func(c *gin.Context) {
		chunknum := c.Param("chunknum")
		num, err := strconv.Atoi(chunknum)
		if err != nil {
			datanode.ZapLogger.Fatalf("%s DataNode error(getchunkhash) at Atoi parse chunknum to int:%v", datanode.Location, err.Error())
		}

		fdata := readFileByBytes(datanode.DATANODE_DIR + "/achunkhashs/chunkhash-" + strconv.Itoa(num))
		c.String(http.StatusOK, string(fdata))
	})

	router.DELETE("/delchunk/:chunknum", func(c *gin.Context) {
		chunknum := c.Param("chunknum")
		num, err := strconv.Atoi(chunknum)
		if err != nil {
			datanode.ZapLogger.Fatalf("%s DataNode error at Atoi parse chunknum to int: %v", datanode.Location, err.Error())
		}

		CleanFile(datanode.DATANODE_DIR + "/chunk/chunk-" + strconv.Itoa(num))
		CleanFile(datanode.DATANODE_DIR + "/achunkhashs/chunkhash-" + strconv.Itoa(num))

		c.String(http.StatusOK, "delete DataNode{*}/chunk/chunk-"+strconv.Itoa(num)+" SUCCESS")
	})

	router.GET("/getmeta", func(c *gin.Context) {
		c.JSON(http.StatusOK, datanode)
	})

	//接收来自namenode的请求
	router.POST("/fixchunk", func(c *gin.Context)  {
		d, _ := c.GetRawData()
		rm := &ReplicaLocation{}
		if len(d) == 0 {
			fmt.Println("put request body为空")
		}
		if err := json.Unmarshal(d, &rm); err != nil {
			fmt.Println("namenode put json to byte error", err)
		}

		//拿到新的DN位置后，将对应的文件块发送给新DN
		buf := new(bytes.Buffer)
		writer := multipart.NewWriter(buf)
		chunkPath := datanode.DATANODE_DIR + "/chunk/chunk-" +  strconv.Itoa(rm.ReplicaNum)

		formFile, err := writer.CreateFormFile("addchunk", chunkPath)
		if err != nil {
			fmt.Println("client error at Create form file", err.Error())
			datanode.ZapLogger.Error("client error: ", err)
		}

		srcFile, err := os.Open(chunkPath)
		if err != nil {
			datanode.ZapLogger.Error("client error at Open source file", err.Error())
		}
		defer srcFile.Close()

		_, err = io.Copy(formFile, srcFile)
		if err != nil {
			datanode.ZapLogger.Error("client error at Write to form file", err.Error())
		}

		params := map[string]string{
			"ReplicaNum": strconv.Itoa(rm.OldNum),
		}
		for key, val := range params {
			err = writer.WriteField(key, val)
			if err != nil {
				datanode.ZapLogger.Error("client error at Set Params", err.Error())
			}
		}

		contentType := writer.FormDataContentType()
		writer.Close()
		
		resp, err := http.Post(rm.ServerLocation + "/addnewchunk", contentType, buf)
		if err != nil {
			fmt.Println("http post error", err)
		}
		defer resp.Body.Close()

		response, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			datanode.ZapLogger.Error("DataNode error at Read response", err.Error())
		}
		fmt.Print("*** DataNode Response: ", string(response))
	})

	//新DN接收来自其他DN的请求
	//接收到请求后，新DN在本地建立一个新的相同的chunk，以及对应的哈希
	router.POST("/addnewchunk", func(c *gin.Context) {
		ReplicaNum := c.PostForm("ReplicaNum")
		datanode.ZapLogger.Infof("* ReplicaNum=%s", ReplicaNum)

		file, _, err := c.Request.FormFile("addchunk")
		if err != nil {
			c.String(http.StatusBadRequest, "XXX Bad request")
			datanode.ZapLogger.Fatalf("%s DataNode error: %v", datanode.Location, err)
			return
		}

		chunkout, err := os.Create(datanode.DATANODE_DIR + "/chunk/chunk-" + ReplicaNum)
		if err != nil {
			fmt.Println("XXX DataNode error at Create chunk file", err.Error())
			datanode.ZapLogger.Fatalf("%s DataNode error at Create chunk file:%v", datanode.Location, err.Error())
		}
		defer chunkout.Close()

		io.Copy(chunkout, file)

		chunkdata := readFileByBytes(datanode.DATANODE_DIR + "/chunk/chunk-" + ReplicaNum)

		hash := sha256.New()
		hash.Write(chunkdata)
		hashStr := hex.EncodeToString(hash.Sum(nil))
		datanode.ZapLogger.Infof("chunk hash %s, hashStr %s", ReplicaNum, hashStr)
		FastWrite(datanode.DATANODE_DIR+"/achunkhashs/chunkhash-"+ReplicaNum, []byte(hashStr))

		n := datanode.StorageAvail
		datanode.ChunkAvail[0] = datanode.ChunkAvail[n-1]
		datanode.ChunkAvail = datanode.ChunkAvail[0 : n-1]
		datanode.StorageAvail--

		fmt.Printf("每个chunk大小:%d Byte, 剩余可用chunk:%d, 总chunk:%d\n",SPLIT_UNIT, datanode.StorageAvail, datanode.StorageTotal)

		c.String(http.StatusCreated, "AddChunk SUCCESS\n")
	})
	router.Run(":" + strconv.Itoa(datanode.Port))
}

//心跳上报
func (datanode *DataNode) SendHeartbeat() {
	defer func() {
		if x := recover(); x != nil {
			datanode.ZapLogger.Fatalf("panic when DataNode %s send heartbeat to namenode, err: %v", datanode.Location, x)
		}
	}()

	//每15s上报一次
	ticker := time.NewTicker(15 * time.Second)
	for{
			<- ticker.C
			datanode.ZapLogger.Infof("datanode %s 上传心跳", datanode.Location)
			d, err := json.Marshal(datanode)
			if err != nil {
				datanode.ZapLogger.Fatalf("%s Datanode send heartbeat json to byte[] error: %v", datanode.Location, err.Error())
			}
			reader := bytes.NewReader(d)
			resp, err := http.Post(datanode.NNLocation[0]+"/heartbeat", "application/json", reader)
			if err != nil {
				datanode.ZapLogger.Fatalf("%s Datanode send heartbeat http post error: %v", datanode.Location, err.Error())
			}
			defer resp.Body.Close()
			datanode.ZapLogger.Infof("datanode %s 上传心跳完毕,收到回复%s", datanode.Location, resp.Status)
		}
	}

func (datanode *DataNode) SetConfig(port string) {

	datanode.ZapLogger = InitLogger(datanode.DATANODE_DIR + "/DNLog.log")

	//所有NN地址
	dnlocations := []string{"http://localhost:11090"}

	res, err := strconv.Atoi(port)
	if err != nil {
		datanode.ZapLogger.Fatalf("%s DataNode error at Atoi parse Port:%v", port, err)
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

	datanode.ZapLogger.Info("************************************************************")
	datanode.ZapLogger.Info("************************************************************")
	datanode.ZapLogger.Info("*** Successfully Set Config data for a datanode")
	datanode.ShowInfo()
	datanode.ZapLogger.Info("************************************************************")
	datanode.ZapLogger.Info("************************************************************")
	go datanode.SendHeartbeat()
}

//目前是datanode断电就删除原来的数据
func (datanode *DataNode) Reset() {
	datanode.reset(datanode.DATANODE_DIR + "/chunk")
	datanode.reset(datanode.DATANODE_DIR + "/achunkhashs")
}

func (datanode *DataNode) reset(dir string) {
	exist, err := PathExists(dir)
	if err != nil {
		datanode.ZapLogger.Fatalf("%s DataNode error at Get Dir: %v", datanode.Location, err)
	}

	if !exist {
		err = os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			datanode.ZapLogger.Fatalf("%s DataNode error at MkdirAll: %v ", datanode.Location, err)
		}
	} else {
		err := os.RemoveAll(dir)
		if err != nil {
			datanode.ZapLogger.Fatalf("%s DataNode error at RemoveAll file hash data: %v ", datanode.Location, err)
		}

		err = os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			datanode.ZapLogger.Fatalf("%s DataNode error at MkdirAll: %v ", datanode.Location, err)
		}
	}
}

func (datanode *DataNode) ShowInfo() {
	datanode.ZapLogger.Infof("Location: %s", datanode.Location)
	datanode.ZapLogger.Infof("DATANODE_DIR: %s", datanode.DATANODE_DIR)
	datanode.ZapLogger.Infof("Port: %d", datanode.Port)
	datanode.ZapLogger.Infof("StorageTotal: %d", datanode.StorageTotal)
	datanode.ZapLogger.Infof("StorageAvail: %d", datanode.StorageAvail)
	datanode.ZapLogger.Infof("ChunkAvail: %d", datanode.ChunkAvail)
	datanode.ZapLogger.Infof("LastEdit: %d", datanode.LastEdit)
}
