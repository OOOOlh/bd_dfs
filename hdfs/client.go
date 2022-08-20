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
)

func (client *Client) PutFile(localPath string, remotePath string) {
	//删除上次的临时文件
	_, err := os.Stat(client.TempStoreLocation)
	//如果存在，就移除
	if err == nil {
		err := os.RemoveAll(client.TempStoreLocation)
		if err != nil {
			fmt.Println("XXX Client error at remove tempfiles", err.Error())
			TDFSLogger.Fatal("XXX Client error: ", err)
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

	//如果发现存在一模一样的文件，就直接返回
	// if len(file.Chunks) == 0{
	// 	fmt.Println("发现相同文件，返回")
	// 	return
	// }

	//创建目录
	err = os.MkdirAll(client.TempStoreLocation+"/"+file.Name, 0777)
	if err != nil {
		fmt.Println("XXX Client error at MkdirAll", err.Error())
		TDFSLogger.Fatal("XXX NameNode error: ", err)
	}

	//开始向DN传数据
	data := readFileByBytes(localPath)
	for i := 0; i < len(file.Chunks); i++ {
		// 存储在缓冲区中
		CreateFile(client.TempStoreLocation + "/" + file.Name + "/chunk-" + strconv.Itoa(i))
		//先在客户端本地分割
		if len(data) < SPLIT_UNIT {
			FastWrite(client.TempStoreLocation+"/"+file.Name+"/chunk-"+strconv.Itoa(i), data[i*SPLIT_UNIT:])
		} else {
			FastWrite(client.TempStoreLocation+"/"+file.Name+"/chunk-"+strconv.Itoa(i), data[i*SPLIT_UNIT:(i+1)*SPLIT_UNIT])
		}
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
			fmt.Println("XXX Client error at remove tempfiles", err.Error())
			TDFSLogger.Fatal("XXX Client error: ", err)
		}
	}
	// err := os.RemoveAll(client.TempStoreLocation)
	// if err!=nil {
	// 	fmt.Println("XXX Client error at remove tempfiles", err.Error())
	// 	TDFSLogger.Fatal("XXX Client error: ", err)
	// }
	fmt.Println("****************************************")
	fmt.Printf("*** Getting from TDFS [NameNode: %s] to ${GOPATH}/%s )\n", client.NameNodeAddr, fName) //  as %s , fName

	response, err := http.Get(client.NameNodeAddr + "/getfile?filename=" + fName)
	if err != nil {
		fmt.Println("XXX Client error at Get file", err.Error())
		TDFSLogger.Fatal("XXX Client error at Get file", err)
	}
	if response.StatusCode == http.StatusNotFound {
		fmt.Printf("Client file=%v not found\n", fName)
		TDFSLogger.Printf("Client file=%v not found", fName)
		return
	}
	defer response.Body.Close()

	bytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println("XXX Client error at read response data", err.Error())
		TDFSLogger.Fatal("XXX Client error at read response data", err)
	}

	file := &File{}
	err = json.Unmarshal(bytes, file)
	if err != nil {
		fmt.Println("XXX Client error at decode json", err.Error())
		TDFSLogger.Fatal("XXX Client error at decode json", err)
	}

	fmt.Println("****************************************")

	err = os.MkdirAll(client.TempStoreLocation+"/"+file.Name, 0777)
	if err != nil {
		fmt.Println("XXX Client error at MkdirAll", err.Error())
		TDFSLogger.Fatal("XXX NameNode error: ", err)
	}

	for i := 0; i < len(file.Chunks); i++ {
		CreateFile(client.TempStoreLocation + "/" + file.Name + "/chunk-" + strconv.Itoa(i))
		//从DN接收文件，并保存在临时文件夹中
		client.GetChunk(file, i)
	}

	/* 将文件夹下的块数据整合成一个文件 */
	client.AssembleFile(*file)
}
func (client *Client) Mkdir(curPath string, folderName string) bool {
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
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("ioutil.ReadAll", err)
	}

	fmt.Println(body)
	return true
}

//new added
func (client *Client) GetFolder(fName string) { //fName string

	fmt.Println("****************************************")
	fmt.Printf("*** Getting from TDFS [NameNode: %s] to ${GOPATH}/%s )\n", client.NameNodeAddr, fName) //  as %s , fName

	//response, err := http.Get(client.NameNodeAddr + "/getfolder/" + fName)

	data := map[string]string{"fname": fName}
	d, err := json.Marshal(data)
	if err != nil {
		fmt.Println("json to byte[] error", err)
	}
	reader := bytes.NewReader(d)
	response, err := http.Post(client.NameNodeAddr+"/getfolder", "application/json", reader)

	if err != nil {
		fmt.Println("Client error at Get folder", err.Error())
		TDFSLogger.Fatal("Client error at Get folder", err)
	}
	defer response.Body.Close()

	bytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println("Client error at read response data", err.Error())
		TDFSLogger.Fatal("Client error at read response data", err)
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
			TDFSLogger.Fatal("XXX Client error: ", err)
		}
	}
	fmt.Println("****************************************")
	fmt.Printf("*** Deleting from TDFS [NameNode: %s] of /%s )\n", client.NameNodeAddr, fName)

	response, err := http.Get(client.NameNodeAddr + "/delfile/" + fName)
	if err != nil {
		fmt.Println("XXX Client error at Get file", err.Error())
		TDFSLogger.Fatal("XXX Client error at Get file", err)
	}
	defer response.Body.Close()

	// Read Response Body
	bytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println("XXX Client error at read response data", err.Error())
		TDFSLogger.Fatal("XXX Client error at read response data", err)
	}

	file := &File{}
	if err = json.Unmarshal(bytes, file); err != nil {
		fmt.Println("byte[] to json error", err)
	}

	for i := 0; i < len(file.Chunks); i++ {
		client.delChunk(file, i)
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
			fmt.Println("XXX NameNode error at Create form file", err.Error())
			TDFSLogger.Fatal("XXX NameNode error: ", err)
		}

		/** Open source file **/
		srcFile, err := os.Open(tempChunkPath)
		if err != nil {
			fmt.Println("XXX NameNode error at Open source file", err.Error())
			TDFSLogger.Fatal("XXX NameNode error: ", err)
		}
		defer srcFile.Close()

		/** Write to form file **/
		_, err = io.Copy(formFile, srcFile)
		if err != nil {
			fmt.Println("XXX NameNode error at Write to form file", err.Error())
			TDFSLogger.Fatal("XXX NameNode error: ", err)
		}

		/** Set Params Before Post **/
		params := map[string]string{
			"ReplicaNum": strconv.Itoa(replicationList[i].ReplicaNum), //chunkNum
		}
		for key, val := range params {
			err = writer.WriteField(key, val)
			if err != nil {
				fmt.Println("XXX NameNode error at Set Params", err.Error())
				TDFSLogger.Fatal("XXX NameNode error: ", err)
			}
		}

		contentType := writer.FormDataContentType()
		writer.Close() // 发送之前必须调用Close()以写入结尾行

		res, err := http.Post(replicationList[i].ServerLocation+"/putchunk",
			contentType, buf) // /"+strconv.Itoa(chunkNum)
		if err != nil {
			fmt.Println("XXX NameNode error at Post form file", err.Error())
			TDFSLogger.Fatal("XXX NameNode error: ", err)
		}
		defer res.Body.Close()

		fmt.Println("** Post form file OK ")

		/** Read response **/
		response, err := ioutil.ReadAll(res.Body)
		if err != nil {
			fmt.Println("XXX NameNode error at Read response", err.Error())
			TDFSLogger.Fatal("XXX NameNode error: ", err)
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
			fmt.Println("XXX NameNode error at Get chunk of ", file.Info, ": ", err.Error())
			TDFSLogger.Fatal("XXX NameNode error: ", err)
		}
		defer dataResp.Body.Close()
		/* deal response of Get */
		chunkbytes, err := ioutil.ReadAll(dataResp.Body)
		if err != nil {
			fmt.Println("XXX NameNode error at ReadAll response of chunk", err.Error())
			TDFSLogger.Fatal("XXX NameNode error: ", err)
		}
		// fmt.Println("** DataNode Response of Get chunk-",num,": ", string(chunkbytes))
		/* store chunkdata at nn local */

		FastWrite(client.TempStoreLocation+"/"+file.Name+"/chunk-"+strconv.Itoa(num), chunkbytes)

		/* send Get chunkhash request */
		hashRes, err := http.Get(replicalocation + "/getchunkhash/" + strconv.Itoa(repilcanum))
		if err != nil {
			fmt.Println("XXX NameNode error at Get chunkhash", err.Error())
			TDFSLogger.Fatal("XXX NameNode error: ", err)
		}
		defer hashRes.Body.Close()
		/* deal Get chunkhash request */
		chunkhash, err := ioutil.ReadAll(hashRes.Body)
		if err != nil {
			fmt.Println("XXX NameNode error at Read response of chunkhash", err.Error())
			TDFSLogger.Fatal("XXX NameNode error: ", err)
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
		fmt.Println("XXX Client error at MkdirAll", err.Error())
		TDFSLogger.Fatal("XXX NameNode error: ", err)
	}
	//只创建文件
	CreateFile(client.StoreLocation + "/" + file.Name + "/" + file.Name)
	FastWrite(client.StoreLocation+"/"+file.Name+"/"+file.Name, fdata)
}

func (client *Client) delChunk(file *File, num int) {
	fmt.Println("** deleting chunk-", num, "of file:", file.Name)

	for i := 0; i < REDUNDANCE; i++ {
		chunklocation := file.Chunks[num].ReplicaLocationList[i].ServerLocation
		chunknum := file.Chunks[num].ReplicaLocationList[i].ReplicaNum
		url := chunklocation + "/delchunk/" + strconv.Itoa(chunknum)

		// response, err := http.Get(url)
		c := &http.Client{}
		req, err := http.NewRequest("DELETE", url, nil)
		if err != nil {
			fmt.Println("XXX NameNode error at Del chunk of ", file.Info, ": ", err.Error())
			TDFSLogger.Fatal("XXX NameNode error: ", err)
		}

		response, err := c.Do(req)
		if err != nil {
			fmt.Println("XXX NameNode error at Del chunk(Do):", err.Error())
			TDFSLogger.Fatal("XXX NameNode error at Del chunk(Do):", err)
		}
		defer response.Body.Close()

		/** Read response **/
		delRes, err := ioutil.ReadAll(response.Body)
		if err != nil {
			fmt.Println("XXX NameNode error at Read response", err.Error())
			TDFSLogger.Fatal("XXX NameNode error: ", err)
		}
		fmt.Println("*** DataNode Response of Delete chunk-", num, "replica-", i, ": ", string(delRes))
		// return chunkbytes
	}
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
		fmt.Println("XXX Client error at Create form file", err.Error())
		TDFSLogger.Fatal("XXX Client error at Create form file", err)
	}

	//打开本地文件
	srcFile, err := os.Open(fPath)
	if err != nil {
		fmt.Println("XXX Client error at Open source file", err.Error())
		TDFSLogger.Fatal("XXX Client error at Open source file", err)
	}
	defer srcFile.Close()

	_, err = io.Copy(formFile, srcFile)
	if err != nil {
		fmt.Println("XXX Client error at Write to form file", err.Error())
		TDFSLogger.Fatal("XXX Client error at Write to form file", err)
	}

	contentType := writer.FormDataContentType()
	writer.Close() // 发送之前必须调用Close()以写入结尾行
	//post发送到http://localhost:11090/putfile
	res, err := http.Post(client.NameNodeAddr+"/putfile", contentType, buf)
	if err != nil {
		fmt.Println("XXX Client error at Post form file", err.Error())
		TDFSLogger.Fatal("XXX Client error at Post form file", err)
	}
	defer res.Body.Close()

	content, err := ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Println("XXX Client error at Read response", err.Error())
		TDFSLogger.Fatal("XXX Client error at Read response", err)
	}

	fmt.Println("*** NameNode Response: ", string(content))
}

//http://localhost:11090
func (client *Client) SetConfig(nnaddr string) {
	client.NameNodeAddr = nnaddr
}

/* Allocation or AskInfo
 * POST (fileName string, fileBytes int)
 * Wait ReplicaList []ReplicaLocation   */
func RequestInfo(fileName string, fileBytes int) []ReplicaLocation {
	/* POST and Wait */
	replicaLocationList := []ReplicaLocation{
		{"http://localhost:11091", 3},
		{"http://localhost:11092", 5},
	}
	return replicaLocationList
}

func uploadFileByBody(client *Client, fPath string) {
	file, err := os.Open(fPath)
	if err != nil {
		fmt.Println("XXX Client Fatal error at Open uploadfile", err.Error())
		TDFSLogger.Fatal("XXX Client error at Open uploadfile", err)
	}
	defer file.Close()

	res, err := http.Post(client.NameNodeAddr+"/putfile", "multipart/form-data", file) //
	if err != nil {
		fmt.Println("Client Fatal error at Post", err.Error())
		TDFSLogger.Fatal("XXX Client error at at Post", err)
	}
	defer res.Body.Close()

	content, err := ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Println("Client Fatal error at Read response", err.Error())
		TDFSLogger.Fatal("XXX Client error at Read response", err)
	}
	fmt.Println(string(content))
}
