package hdfs

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"mime/multipart"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func (client *Client) PutFile(localPath string, remotePath string) {
	//删除上次的临时文件
	_, err := os.Stat(client.TempStoreLocation)
	//如果存在，就移除
	if err == nil {
		err := os.RemoveAll(client.TempStoreLocation)
		if err != nil {
			fmt.Println("client error at remove tempfiles", err.Error())
			sugarLogger.Fatalf("client remove tempfiles error:%s ", err)
			// TDFSLogger.Fatal("XXX Client error: ", err)
		}
	}

	fmt.Println("****************************************")
	// fmt.Printf("*** Putting %s to TDFS [NameNode: %s] )\n", fPath, client.NameNodeAddr)

	//将文件的名字和大小发送给NN
	f, err := os.Stat(localPath)
	if err != nil {
		fmt.Println("file state error", err)
	}
	//生成文件hash码，用于校验文件是否改变
	fileBytes := readFileByBytes(localPath)
	//	crc32 := crc32.ChecksumIEEE(fileBytes)
	hash := sha256.New()
	hash.Write(fileBytes)
	hashStr := hex.EncodeToString(hash.Sum(nil))

	file := &File{
		Name:   f.Name(),
		Length: f.Size(),

		Info:       hashStr,
		RemotePath: remotePath,
	}

	// json字符串
	d, err := json.Marshal(file)
	if err != nil {
		fmt.Println("json to byte[] error", err)
	}
	// 序列化
	reader := bytes.NewReader(d)
	resp, err := http.Post(client.NameNodeAddr+"/put", "application/json", reader)
	if err != nil {
		fmt.Println("http post error", err)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("ioutil.ReadAll", err)
	}

	if err = json.Unmarshal(body, file); err != nil {
		fmt.Println("byte[] to json error", err)
	}

	//创建目录
	err = os.MkdirAll(client.TempStoreLocation+"/"+file.Name, 0777)
	if err != nil {
		fmt.Println("client error at MkdirAll", err.Error())
		// TDFSLogger.Fatal("XXX NameNode error: ", err)
		sugarLogger.Fatalf("client makdir error: %s", err)
	}

	//开始向DN传数据
	data := readFileByBytes(localPath)
	for i := 0; i < len(file.Chunks); i++ {
		CreateFile(client.TempStoreLocation + "/" + file.Name + "/chunk-" + strconv.Itoa(i))
		if i == len(file.Chunks)-1 {
			FastWrite(client.TempStoreLocation+"/"+file.Name+"/chunk-"+strconv.Itoa(i), data[i*SPLIT_UNIT:])
		} else {
			FastWrite(client.TempStoreLocation+"/"+file.Name+"/chunk-"+strconv.Itoa(i), data[i*SPLIT_UNIT:(i+1)*SPLIT_UNIT])
		}
		//}
		// 发送到datanode进行存储
		PutChunk(client.TempStoreLocation+"/"+file.Name+"/chunk-"+strconv.Itoa(i), file.Chunks[i].ReplicaLocationList)
	}
	fmt.Println("putFile finish!")
}

func (client *Client) GetFile(fName string) { //, fName string
	//删除上次的临时文件
	_, err := os.Stat(client.TempStoreLocation)
	//如果存在，就移除
	if err == nil {
		err := os.RemoveAll(client.TempStoreLocation)
		if err != nil {
			fmt.Println("client error at remove tempfiles", err.Error())
			// TDFSLogger.Fatal("XXX Client error: ", err)
			sugarLogger.Fatalf("client error at remove tempfiles: %s", err)
		}
	}

	fmt.Println("****************************************")
	fmt.Printf("*** Getting from TDFS [NameNode: %s] to ${GOPATH}/%s )\n", client.NameNodeAddr, fName) //  as %s , fName

	response, err := http.Get(client.NameNodeAddr + "/getfile?filename=" + fName)
	if err != nil {
		fmt.Println("XXX Client error at Get file", err.Error())
		// TDFSLogger.Fatal("XXX Client error at Get file", err)
		sugarLogger.Fatalf("Client error at Get file", err)
	}
	if response.StatusCode == http.StatusNotFound {
		fmt.Printf("Client file=%v not found\n", fName)
		// TDFSLogger.Printf("Client file=%v not found", fName)
		sugarLogger.Warnf("Client file=%s not found", fName)
		return
	}
	defer response.Body.Close()

	bytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println("client error at read response data", err.Error())
		// TDFSLogger.Fatal("XXX Client error at read response data", err)
		sugarLogger.Fatalf("client error at read response data:%s", err)
	}

	file := &File{}
	err = json.Unmarshal(bytes, file)
	if err != nil {
		fmt.Println("client error at decode json", err.Error())
		// TDFSLogger.Fatal("XXX Client error at decode json", err)
		sugarLogger.Fatalf("client error at decode json:%s", err)
	}

	fmt.Println("****************************************")

	err = os.MkdirAll(client.TempStoreLocation+"/"+file.Name, 0777)
	if err != nil {
		sugarLogger.Fatalf("client mkdir error:%s", err)
		fmt.Println("XXX Client error at MkdirAll", err.Error())
		// TDFSLogger.Fatal("XXX NameNode error: ", err)
	}

	for i := 0; i < len(file.Chunks); i++ {
		CreateFile(client.TempStoreLocation + "/" + file.Name + "/chunk-" + strconv.Itoa(i))
		//从DN接收文件，并保存在临时文件夹中
		client.GetChunk(file, i)
	}
	/* 将文件夹下的块数据整合成一个文件 */
	client.AssembleFile(*file)
}
func (client *Client) Mkdir(curPath string, folderName string) {
	dataMap := map[string]string{}
	dataMap["curPath"] = curPath
	dataMap["folderName"] = folderName
	d, err := json.Marshal(dataMap)
	if err != nil {
		fmt.Println("json to byte[] error", err)
	}
	// 序列化
	reader := bytes.NewReader(d)
	resp, err := http.Post(client.NameNodeAddr+"/mkdir", "application/json", reader)
	if err != nil {
		fmt.Println("http post error", err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("ioutil.ReadAll", err)
	}
	var res []bool
	if err = json.Unmarshal(body, &res); err != nil {
		fmt.Println("byte[] to json error", err)
	}
	fmt.Println("success mkdir the folder:", res)
}
func (client *Client) ReNameFolder(preFolder string, reNameFolder string) {
	data := map[string]string{"preFolder": preFolder, "reNameFolder": reNameFolder}
	d, err := json.Marshal(data)
	if err != nil {
		fmt.Println("json to byte[] error", err)
	}
	reader := bytes.NewReader(d)
	response, err := http.Post(client.NameNodeAddr+"/reFolderName", "application/json", reader)
	if err != nil {
		sugarLogger.Error(err)
	}
	defer response.Body.Close()
	bytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		sugarLogger.Error(err)
	}
	var res bool
	if err = json.Unmarshal(bytes, &res); err != nil {
		fmt.Println("byte[] to json error", err)
	}
	fmt.Println(res)
}

// 获取指定目录下的目录列表
func (client *Client) GetCurPathFolder(folderPath string) {
	data := map[string]string{"fname": folderPath}
	d, err := json.Marshal(data)
	if err != nil {
		fmt.Println("json to byte[] error", err)
	}
	reader := bytes.NewReader(d)
	response, err := http.Post(client.NameNodeAddr+"/getFolders", "application/json", reader)
	if err != nil {
		sugarLogger.Error(err)
	}
	defer response.Body.Close()
	bytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		sugarLogger.Error(err)
	}
	var folder []string
	if err = json.Unmarshal(bytes, &folder); err != nil {
		fmt.Println("byte[] to json error", err)
	}
	fmt.Println("success get the filename list in this folder:", folder)
}

func StartNewDataNode(c []string) {
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

// 获取文件元位置信息
func (client *Client) GetFileStat(fName string) { //fName string
	fmt.Println("****************************************")
	fmt.Printf("*** Getting File Stat from BD_DFS [NameNode: %s] to ${GOPATH}/%s )\n", client.NameNodeAddr, fName) //  as %s , fName

	data := map[string]string{"fname": fName}
	d, err := json.Marshal(data)
	if err != nil {
		fmt.Println("json to byte[] error", err)
	}
	reader := bytes.NewReader(d)
	response, err := http.Post(client.NameNodeAddr+"/getfilestat", "application/json", reader)

	if err != nil {
		fmt.Println("Client error at Get folder", err.Error())
	}
	defer response.Body.Close()

	bytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println("Client error at read response data", err.Error())
	}

	var folder []string
	if err = json.Unmarshal(bytes, &folder); err != nil {
		fmt.Println("byte[] to json error", err)
	}

	fmt.Println("success get the file info :", data)

}

// 节点扩容
func (client *Client) ExpandNode(nodeDir, nodePort string) {
	fmt.Printf("节点准备扩容, 创建新结点：%s, %s", nodeDir, nodePort)
	fmt.Println()
	c := []string{"dn.exe", "-dir", nodeDir, "-port", nodePort}
	StartNewDataNode(c)
	fmt.Println("获取所有文件的当前地址")
	response, err := http.Get(client.NameNodeAddr + "/getFilesChunkLocation")
	if err != nil {
		fmt.Println("Client error get FilesChunkLocation !")
	}
	defer response.Body.Close()
	res, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println("Client error at read response data")
	}
	var FileChunks []FileChunkResponse
	if err = json.Unmarshal(res, &FileChunks); err != nil {
		fmt.Println("byte[] to json error", err)
	}
	// 节点均衡操作，将一些块移动到新加入的节点上去
	// 划分新的数据到新节点上, 取数据的第一个块对应的第一个冗余块(一个两个冗余块)到新结点上去
	transferChunk := map[string][]string{}
	for _, fileChunk := range FileChunks {
		preLocation := fileChunk.Chunks[0].ReplicaLocationList[0].ServerLocation
		preReplicaNum := strconv.Itoa(fileChunk.Chunks[0].ReplicaLocationList[0].ReplicaNum)
		transferChunk[fileChunk.Path] = []string{preLocation, preReplicaNum}
	}
	for key, value := range transferChunk {
		fmt.Printf("need transfer file %s block %s,%s to new Node!", key, value[0], value[1])
		fmt.Println()
	}
	// 获取移动节点的数据
	index := 0
	dataNewNode := map[string]int{}

	updateFile := []string{}
	for path, value := range transferChunk {
		updateFile = append(updateFile, path)
		response2, err2 := http.Get(value[0] + "/getchunk/" + value[1])
		defer response.Body.Close()
		if err2 != nil {
			fmt.Println("Client error get FilesChunkLocation !")
		}
		re, err3 := ioutil.ReadAll(response2.Body)
		if err3 != nil {
			fmt.Println("Client error at read response data")
		}
		// 发送数据到datanode上进行存储
		url := "http://localhost:" + nodePort + "/putChunkBybytes"
		data := map[string][]byte{"data": re, "chunkId": []byte(strconv.Itoa(index))}
		d, err2 := json.Marshal(data)
		if err2 != nil {
			fmt.Println("json to byte[] error", err2)
		}
		reader2 := bytes.NewReader(d)
		response3, err3 := http.Post(url, "application/json", reader2)
		if err3 != nil {
			fmt.Println("put chunk error !")
		}
		defer response3.Body.Close()
		fmt.Println(response3)
		dataNewNode[path] = index
		index += 1
	}
	// 更新nameNode节点
	updateNN := map[string][]string{}
	updateNN["filePath"] = updateFile
	updateNN["newNode"] = []string{nodeDir, nodePort}
	d, err2 := json.Marshal(updateNN)
	if err2 != nil {
		fmt.Println("json to byte[] error", err2)
	}
	reader2 := bytes.NewReader(d)
	response4, err4 := http.Post(client.NameNodeAddr+"/updataNewNode", "application/json", reader2)
	if err4 != nil {
		fmt.Println("get update response error!")
	}
	response4.Body.Close()
	if err != nil {
		fmt.Println("update NN error !")
	}
	fmt.Println(response)
}

//new added
func (client *Client) GetFiles(fName string) { //fName string
	fmt.Println("****************************************")
	fmt.Printf("*** Getting from TDFS [NameNode: %s] to ${GOPATH}/%s )\n", client.NameNodeAddr, fName) //  as %s , fName
	//response, err := http.Get(client.NameNodeAddr + "/getfolder/" + fName)
	data := map[string]string{"fname": fName}
	d, err := json.Marshal(data)
	if err != nil {
		fmt.Println("json to byte[] error", err)
	}
	reader := bytes.NewReader(d)
	response, err := http.Post(client.NameNodeAddr+"/getFiles", "application/json", reader)
	if err != nil {
		sugarLogger.Error(err)
	}
	defer response.Body.Close()
	bytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		sugarLogger.Error(err)
	}
	var folder []string
	if err = json.Unmarshal(bytes, &folder); err != nil {
		fmt.Println("byte[] to json error", err)
	}
	fmt.Println("success get the filename list in this folder:", folder)
}

func (client *Client) DelFile(fName string) {
	//删除上次的临时文件
	_, err := os.Stat(client.TempStoreLocation)
	//如果存在，就移除
	if err == nil {
		err := os.RemoveAll(client.TempStoreLocation)
		if err != nil {
			fmt.Println("XXX Client error at remove tempfiles", err.Error())
		}
	}
	fmt.Println("****************************************")
	fmt.Printf("*** Deleting from TDFS [NameNode: %s] of /%s )\n", client.NameNodeAddr, fName)

	data := map[string]string{"filename": fName}
	d, err := json.Marshal(data)
	if err != nil {
		fmt.Println("json to byte[] error", err)
	}
	reader := bytes.NewReader(d)
	response, err := http.Post(client.NameNodeAddr+"/delfile", "application/json", reader)
	if err != nil {
		fmt.Println("Client error at Delete File", err.Error())
	}
	defer response.Body.Close()
	_, err = ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println("Client error at read response data", err.Error())
	}

	fmt.Println("*** delete finish ")
	fmt.Println("****************************************")
}

//将文件的该chunk发送给对应的两个DN
func PutChunk(tempChunkPath string, replicationList [REDUNDANCE]ReplicaLocation) {
	for i := 0; i < len(replicationList); i++ {
		// replication[i].ServerLocation
		/** Create form file **/
		buf := new(bytes.Buffer)
		writer := multipart.NewWriter(buf)
		formFile, err := writer.CreateFormFile("putchunk", tempChunkPath)
		if err != nil {
			sugarLogger.Error(err)
		}

		/** Open source file **/
		srcFile, err := os.Open(tempChunkPath)
		if err != nil {
			sugarLogger.Error(err)
		}
		defer srcFile.Close()

		/** Write to form file **/
		_, err = io.Copy(formFile, srcFile)
		if err != nil {
			sugarLogger.Error(err)
		}

		/** Set Params Before Post **/
		params := map[string]string{
			"ReplicaNum": strconv.Itoa(replicationList[i].ReplicaNum), //chunkNum
		}
		for key, val := range params {
			err = writer.WriteField(key, val)
			if err != nil {
				sugarLogger.Error(err)
			}
		}

		contentType := writer.FormDataContentType()
		writer.Close() // 发送之前必须调用Close()以写入结尾行

		fmt.Println(replicationList[i].ServerLocation + "/putchunk")
		res, err := http.Post(replicationList[i].ServerLocation+"/putchunk",
			contentType, buf) // /"+strconv.Itoa(chunkNum)
		if err != nil {
			sugarLogger.Error(err)
		}
		defer res.Body.Close()

		fmt.Println("** Post form file OK ")

		/** Read response **/
		response, err := ioutil.ReadAll(res.Body)
		if err != nil {
			sugarLogger.Error(err)
		}
		fmt.Print("*** DataNoed Response: ", string(response))
	}
}

func (client *Client) GetChunk(file *File, num int) { //ChunkUnit chunkbytes []byte

	fmt.Println("* getting chunk-", num, "of file:", file.Name)

	for i := 0; i < REDUNDANCE; i++ {
		replicalocation := file.Chunks[num].ReplicaLocationList[i].ServerLocation
		repilcanum := file.Chunks[num].ReplicaLocationList[i].ReplicaNum
		/* send Get chunkdata request */
		url := replicalocation + "/getchunk/" + strconv.Itoa(repilcanum)
		dataResp, err := http.Get(url)
		if err != nil {
			sugarLogger.Error(err)
			continue
		}
		defer dataResp.Body.Close()
		/* deal response of Get */
		chunkbytes, err := ioutil.ReadAll(dataResp.Body)
		if err != nil {
			sugarLogger.Error(err)
		}
		// fmt.Println("** DataNode Response of Get chunk-",num,": ", string(chunkbytes))
		/* store chunkdata at nn local */

		FastWrite(client.TempStoreLocation+"/"+file.Name+"/chunk-"+strconv.Itoa(num), chunkbytes)

		/* send Get chunkhash request */
		hashRes, err := http.Get(replicalocation + "/getchunkhash/" + strconv.Itoa(repilcanum))
		if err != nil {
			sugarLogger.Error(err)
		}
		defer hashRes.Body.Close()
		/* deal Get chunkhash request */
		chunkhash, err := ioutil.ReadAll(hashRes.Body)
		if err != nil {
			sugarLogger.Error(err)
		}

		/* check hash */
		// chunkfile := OpenFile(namenode.NAMENODE_DIR+"/"+filename+"/chunk-"+strconv.Itoa(chunknum))
		hash := sha256.New()
		hash.Write(chunkbytes)
		// if _, err := io.Copy(hash, chunkfile); err != nil {fmt.Println("XXX NameNode error at sha256", err.Error())}
		hashStr := hex.EncodeToString(hash.Sum(nil))
		fmt.Println("*** chunk hash calculated: ", hashStr)
		fmt.Println("*** chunk hash get: ", string(chunkhash))

		if hashStr == string(chunkhash) {
			break
		} else {
			fmt.Println("X=X the first replica of chunk-", num, "'s hash(checksum) is WRONG, continue to request anothor replica...")
			continue
		}
	}
}

func (client *Client) AssembleFile(file File) {
	fmt.Println("@ AssembleFile of ", file.Name)
	filedata := make([][]byte, len(file.Chunks))
	for i := 0; i < len(file.Chunks); i++ {
		// fmt.Println("& Assmble chunk-",i)
		b := readFileByBytes(client.TempStoreLocation + "/" + file.Name + "/chunk-" + strconv.Itoa(i))
		// fmt.Println("& Assmble chunk b=", b)
		filedata[i] = make([]byte, 100)
		filedata[i] = b
		// fmt.Println("& Assmble chunk filedata[i]=", filedata[i])
		// fmt.Println("& Assmble chunk-",i)
	}
	fdata := bytes.Join(filedata, nil)

	//只创建文件夹
	err := os.MkdirAll(client.StoreLocation+"/"+file.Name, 0777)
	if err != nil {
		sugarLogger.Error(err)
	}

	d := strings.Split(file.RemotePath, "/")[1:]
	var newDir string
	for _, s := range d {
		newDir = newDir + s + "-"
	}

	dir := client.StoreLocation + "/" + file.Name + "/" + newDir + file.Name
	//只创建文件
	CreateFile(dir)
	FastWrite(dir, fdata)
}

//client要求NameNode的两个DataNode删除chunk
func (client *Client) delChunk(file *File, num int) {
	fmt.Println("** deleting chunk-", num, "of file:", file.Name)
	var wg sync.WaitGroup
	wg.Add(REDUNDANCE)
	for i := 0; i < REDUNDANCE; i++ {
		go func(i int) {
			chunklocation := file.Chunks[num].ReplicaLocationList[i].ServerLocation
			chunknum := file.Chunks[num].ReplicaLocationList[i].ReplicaNum
			url := chunklocation + "/delchunk/" + strconv.Itoa(chunknum)

			// response, err := http.Get(url)
			c := &http.Client{}
			req, err := http.NewRequest("DELETE", url, nil)
			if err != nil {
				sugarLogger.Error(err)
			}

			response, err := c.Do(req)
			if err != nil {
				sugarLogger.Error(err)
			}
			defer response.Body.Close()

			/** Read response **/
			delRes, err := ioutil.ReadAll(response.Body)
			if err != nil {
				sugarLogger.Error(err)
			}
			fmt.Println("*** DataNode Response of Delete chunk-", num, "replica-", i, ": ", string(delRes))
			// return chunkbytes
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func (client *Client) Test() {
	_, err := http.Get(client.NameNodeAddr + "/test")
	if err != nil {
		fmt.Println("Client error at Get test", err.Error())
	}
}

func (client *Client) uploadFileByMultipart(fPath string) {
	buf := new(bytes.Buffer)
	writer := multipart.NewWriter(buf)
	formFile, err := writer.CreateFormFile("putfile", fPath)
	if err != nil {
		sugarLogger.Error(err)
	}

	//打开本地文件
	srcFile, err := os.Open(fPath)
	if err != nil {
		sugarLogger.Error(err)
	}
	defer srcFile.Close()

	_, err = io.Copy(formFile, srcFile)
	if err != nil {
		sugarLogger.Error(err)
	}

	contentType := writer.FormDataContentType()
	writer.Close() // 发送之前必须调用Close()以写入结尾行
	//post发送到http://localhost:11090/putfile
	res, err := http.Post(client.NameNodeAddr+"/putfile", contentType, buf)
	if err != nil {
		sugarLogger.Error(err)
	}
	defer res.Body.Close()

	content, err := ioutil.ReadAll(res.Body)
	if err != nil {
		sugarLogger.Error(err)
	}

	fmt.Println("*** NameNode Response: ", string(content))
}

//http://localhost:11090
func (client *Client) SetConfig(nnaddr ...string) {
	client.AllNameNodeAddr = nnaddr
	rand.Seed(time.Now().Unix())
	addr := nnaddr[rand.Intn(len(nnaddr))]
	res, err := http.Get(addr + "/leader")
	if err != nil {
		fmt.Println("get leader error", err.Error())
		//TDFSLogger.Fatal("get leader error", err)
	}
	defer res.Body.Close()
	body, _ := ioutil.ReadAll(res.Body)
	fmt.Println(string(body))
	client.NameNodeAddr = string(body)
}

/* Allocation or AskInfo
 * POST (fileName string, fileBytes int)
 * Wait ReplicaList []ReplicaLocation   */
// func RequestInfo(fileName string, fileBytes int) []ReplicaLocation {
// 	/* POST and Wait */
// 	replicaLocationList := []ReplicaLocation{
// 		{0, "http://localhost:11091", 3, 0},
// 		{0, "http://localhost:11092", 5, 0},
// 	}
// 	return replicaLocationList
// }

func uploadFileByBody(client *Client, fPath string) {
	file, err := os.Open(fPath)
	if err != nil {
		sugarLogger.Error(err)
	}
	defer file.Close()

	res, err := http.Post(client.NameNodeAddr+"/putfile", "multipart/form-data", file) //
	if err != nil {
		sugarLogger.Error(err)
	}
	defer res.Body.Close()

	content, err := ioutil.ReadAll(res.Body)
	if err != nil {
		sugarLogger.Error(err)
	}
	fmt.Println(string(content))
}
