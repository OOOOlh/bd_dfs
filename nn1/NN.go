package main

import (
	"hdfs/hdfs"
	"time"
)

const NN_DIR string = "./namenode"
const NN_LOCATION string = "http://localhost:11090"

const rEDUNDANCE int = 2
const EXEC string = "dn.exe"

var standBy = [][]string{
	{EXEC, "-dir", "dn4", "-port", "11094"},
	{EXEC, "-dir", "dn5", "-port", "11095"},
}

func main() {
	var nn hdfs.NameNode
	nn.NAMENODE_DIR = NN_DIR
	nn.StandByDataNode = standBy
	nnlocations := []string{"http://localhost:11088", "http://localhost:11089", "http://localhost:11090"}
	c := [][]string{
		{EXEC, "-dir", "dn1", "-port", "11091"},
		{EXEC, "-dir", "dn2", "-port", "11092"},
		{EXEC, "-dir", "dn3", "-port", "11093"},
	}

	var dnlocations []string

	for i := 0; i < len(c); i++ {
		dnlocations = append(dnlocations, "http://localhost:"+c[i][4])
		nn.StartNewDataNode(c[i])
	}
	time.Sleep(time.Second * 3)
	nn.SetConfig(NN_LOCATION, len(c), rEDUNDANCE, dnlocations, nnlocations)
	nn.GetDNMeta() // UpdateMeta
	go nn.RunHeartBeat()
	nn.Run()
}
