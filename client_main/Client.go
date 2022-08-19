package main

import (
	// "bd_hdfs/tdfs"

	"fmt"
	"hdfs/hdfs"

	"flag"
	// "runtime"
	// "sync"
)

func main() {

	/*添加文件*/
	// cd client_main
	// go run Client.go -putfile "绝对路径"或"./(相对路径，与Client.go同路径)"
	//例如
	// go run Client.go -putfile "D:/go/workspace/src/auto.jpg"

	/*下载文件*/
	// cd client_main
	// go run Client.go -getfile "文件名"
	//例如
	// go run Client.go -getfile "auto.jpg"

	/*删除文件*/
	// cd client_main
	// go run Client.go -delfile "文件名"
	//例如
	// go run Client.go -delfile "auto.jpg"

	var client hdfs.Client
	client.SetConfig("http://localhost:11090")
	client.StoreLocation = "./dfs"
	client.TempStoreLocation = "./dfs/temp"

	// filenameOfGet := flag.String("getfile", "unknow", "the filename of the file you want to get") // SmallFile
	localFilePath := flag.String("local", "unknow", "local_file_path")
	remoteFilePath := flag.String("remote", "unknow", "remote_file_path")
	// filenameOfPut := flag.String("putfile", "unknow", "the filename of the file you want to put") // SmallFile.txt
	filenameOfDel := flag.String("delfile", "unknow", "the filename of the file you want to del")
	folderNameOfGet := flag.String("getfolder", "unknow", "the name of folder you want to check")
	flag.Parse()

	// if *filenameOfPut != "unknow" {
	// 	client.PutFile(*filenameOfPut)
	// 	fmt.Println(" -PutFile for ", *filenameOfPut)
	// }
	
	//Put
	if *localFilePath != "unknow" && *remoteFilePath != "unknow"{
		client.PutFile(*localFilePath, *remoteFilePath)
		fmt.Printf(" PutFile %s to %s ", *localFilePath, *remoteFilePath)
	}

	// if *filenameOfGet != "unknow" {
	// 	client.GetFile(*filenameOfGet)
	// 	fmt.Println(" -Getfile for ", *filenameOfGet)
	// }

	if *filenameOfDel != "unknow" {
		client.DelFile(*filenameOfDel)
		fmt.Println(" -Delfile for ", *filenameOfDel)
	}

	if *folderNameOfGet != "unknow" {
		client.GetFolder(*folderNameOfGet)
		fmt.Println(" -Getfolder for ", *folderNameOfGet)
	}
}
