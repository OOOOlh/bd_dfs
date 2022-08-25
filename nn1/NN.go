package main

import "hdfs/hdfs"

const NN_DIR string = "./namenode"
const NN_LOCATION string = "http://localhost:11090"
const NN_DNNumber int = 3 // 2
const rEDUNDANCE int = 2

func main() {
	var nn hdfs.NameNode
	nn.NAMENODE_DIR = NN_DIR
	dnlocations := []string{"http://localhost:11091", "http://localhost:11092", "http://localhost:11093"}
	nnlocations := []string{"http://localhost:11088", "http://localhost:11089", "http://localhost:11090"}
	//nn.Reset()
	nn.SetConfig(NN_LOCATION, NN_DNNumber, rEDUNDANCE, dnlocations, nnlocations)
	nn.GetDNMeta() // UpdateMeta
	go nn.RunHeartBeat()
	nn.Run()
}
