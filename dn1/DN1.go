package main

import "hdfs/hdfs"

// import "tidydfs/tdfs"

// "fmt"
// "tdfs"
// "runtime"
// "sync"
const DN1_DIR string = "./datanode"
const DN1_LOCATION string = "11096"
const DN1_CAPACITY int = 400

func main() {
	var dn1 hdfs.DataNode
	dn1.DATANODE_DIR = DN1_DIR
	dn1.Reset()
	// 位置  容量
	dn1.SetConfig(DN1_LOCATION)
	dn1.Run()
}
