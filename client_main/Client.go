package main

import (
	"flag"
	"fmt"
	"hdfs/hdfs"
)

func main() {

	/*添加文件*/
	// cd client_main
	// go run Client.go -local "绝对路径"或"./(相对路径，与Client.go同路径)" -remote "namenode文件路径"

	/*下载文件*/
	// cd client_main
	// go run Client.go -getfile "namenode文件路径"


	/*删除文件*/
	// cd client_main
	// go run Client.go -delfile "文件名"


	var client hdfs.Client

	client.SetConfig("http://localhost:11088", "http://localhost:11089", "http://localhost:11090")

	client.StoreLocation = "./dfs"
	client.TempStoreLocation = "./dfs/temp"

	//文件上传
	localFilePath := flag.String("local", "unknow", "local_file_path")
	remoteFilePath := flag.String("remote", "unknow", "remote_file_path")

	// 获取文件
	filenameOfGet := flag.String("getfile", "unknow", "the filename of the file you want to get")

	//删除
	filenameOfDel := flag.String("delfile", "unknow", "the filename of the file you want to del")

	//获取指定目录下的文件列表
	filesNameOfGet := flag.String("filesNameOfGet", "unknow", "the name of folder you want to check")

	//获取指定目录下的目录列表
	foldersNameOfGet := flag.String("foldersNameOfGet", "unknow", "the name of folder you want to check")

	curFolder := flag.String("curFolder", "unknow", "the folder you want to make")

	//目录下新建目录
	newFolder := flag.String("newFolder", "unknow", "the name of folder you want to check")
	
	//目录下重命名
	reNameFolder := flag.String("reNameFolder", "unknow", "the name of folder you want to check")

	// 节点扩容
	newNodeDir := flag.String("newNodeDir", "unknow", "the newNode dir you want to check")
	newNodePort := flag.String("newNodePort", "unknow", "the newNode newNodePort you want to check")

	//获取文件元数据信息
	fileStatOfGet := flag.String("getfilestat", "unknow", "the info of file you want to search")

	flag.Parse()

	//上传
	if *localFilePath != "unknow" && *remoteFilePath != "unknow" {
		client.PutFile(*localFilePath, *remoteFilePath)
		fmt.Printf(" PutFile %s to %s \n", *localFilePath, *remoteFilePath)
	}

	// 读取
	if *filenameOfGet != "unknow" {
		client.GetFile(*filenameOfGet)
		fmt.Println(" -Getfile for ", *filenameOfGet)
	}

	// 删除
	if *filenameOfDel != "unknow" {
		client.DelFile(*filenameOfDel)
		fmt.Println(" -Delfile for ", *filenameOfDel)
	}

	// 创建目录
	if *curFolder != "unknow" && *newFolder != "unknow" {
		client.Mkdir(*curFolder, *newFolder)
		fmt.Println("-Mkdir for ", *curFolder)
	}

	// 获取指定目录下的文件列表(测试过)
	if *filesNameOfGet != "unknow" {
		client.GetFiles(*filesNameOfGet)
		fmt.Println(" -GetFiles for ", *filesNameOfGet)
	}

	//获取指定目录文件的的目录列表(测试过)
	if *foldersNameOfGet != "unknow" {
		client.GetCurPathFolder(*foldersNameOfGet)
		fmt.Println("-GetFolders for")
	}

	// 对目录进行重命名(测试过)
	if *curFolder != "unknow" && *reNameFolder != "unknow" {
		client.ReNameFolder(*curFolder, *reNameFolder)
		fmt.Printf("-ReName Folder for %s to %s", *curFolder, *reNameFolder)
	}

	// 节点扩容
	if *newNodeDir != "unknow" && *newNodePort != "unknow" {
		client.ExpandNode(*newNodeDir, *newNodePort)
		fmt.Println("success create new node: #{*newNodeDir}, #{*newNodePort}")
	}

	//获取文件元数据信息
	if *fileStatOfGet != "unknow" {
		client.GetFileStat(*fileStatOfGet)
		fmt.Println(" -Getfilestat for ", *fileStatOfGet)
	}
}
