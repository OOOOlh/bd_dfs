package main

import (
	"flag"
	"hdfs/hdfs"
)

func main() {

	storeDir := flag.String("dir",  "unknown", "the dir to store chunk")
	port := flag.String("port", "unknown", "the port to listen")

	flag.Parse()

	var dn hdfs.DataNode
	dn.DATANODE_DIR = *storeDir
	dn.Reset()
	// 位置  容量
	dn.SetConfig(*port)
	dn.Run()
}