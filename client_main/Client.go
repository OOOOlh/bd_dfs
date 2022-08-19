package main

import (
	"flag"
	// "bd_hdfs/tdfs"
	"fmt"
	"hdfs/hdfs"
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
	filenameOfMkdir := flag.String("mkdir", "unknow", "the folder you want to make")
	flag.Parse()

	// 上传
	if *filenameOfPut != "unknow" {
		client.PutFile(*filenameOfPut)
		fmt.Println(" -PutFile for ", *filenameOfPut)
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
	// 获取当前目录文件列表
	// 当前目录下创建目录
	// 根据文件路径获取文件信息

	if *filenameOfMkdir != "unknow" {
		client.Mkdir("/root", "data")
		fmt.Println("-Mkdir for ", *filenameOfMkdir)
	}

	// 文件重命名
	// fmt.Println(flag.Args())
	//, "smallfile.txt" /Users/treasersmac/Programming/MilkPrairie/Gou/TinyDFS/
	// client.Test()
}
