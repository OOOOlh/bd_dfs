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

	filenameOfGet := flag.String("getfile", "unknow", "the filename of the file you want to get") // SmallFile
	filenameOfPut := flag.String("putfile", "unknow", "the filename of the file you want to put") // SmallFile.txt
	filenameOfDel := flag.String("delfile", "unknow", "the filename of the file you want to del")

	flag.Parse()
	
	if *filenameOfPut!="unknow" {
		client.PutFile(*filenameOfPut)
		fmt.Println(" -PutFile for ", *filenameOfPut)
	}
	
	if *filenameOfGet!="unknow" {
		client.GetFile(*filenameOfGet)
		fmt.Println(" -Getfile for ", *filenameOfGet)
	}

	if *filenameOfDel!="unknow" {
		client.DelFile(*filenameOfDel)
		fmt.Println(" -Delfile for ", *filenameOfDel)
	}

	// fmt.Println(flag.Args())
	//, "smallfile.txt" /Users/treasersmac/Programming/MilkPrairie/Gou/TinyDFS/
	// client.Test()
}