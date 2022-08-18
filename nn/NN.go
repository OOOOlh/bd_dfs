package main

import "hdfs/hdfs"

// import "hdfs/tdfs"

// import "tidydfs/tdfs"

// "fmt"
// "tdfs"
// "runtime"
// "sync"

const NN_DIR string = "./namenode"
const NN_LOCATION string = "http://localhost:11090"
const NN_DNNumber int = 3 // 2
const rEDUNDANCE int = 2


func main() {
	var nn hdfs.NameNode
	nn.NAMENODE_DIR = NN_DIR
	dnlocations := []string{"http://localhost:11091", "http://localhost:11092" ,"http://localhost:11093"}

	nn.Reset()
	nn.SetConfig(NN_LOCATION, NN_DNNumber, rEDUNDANCE, dnlocations)
	nn.GetDNMeta() // UpdateMeta

	nn.Run()
}