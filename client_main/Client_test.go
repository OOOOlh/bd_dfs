package main

import (
	"hdfs/hdfs"
	"strconv"
	"testing"
)

var client hdfs.Client

func init() {
	client.SetConfig("http://localhost:11088", "http://localhost:11089", "http://localhost:11090")
	//client.SetConfig( "http://localhost:11089")
	client.StoreLocation = "./dfs"
	client.TempStoreLocation = "./dfs/temp"
}

func BenchmarkPut(b *testing.B) {
	for n := 0; n < b.N; n++ {
		client.PutFile("./test.txt", "/root/"+strconv.FormatInt(int64(n), 10))
	}
}

func BenchmarkGet(b *testing.B) {
	for n := 0; n < b.N; n++ {
		client.GetFile("/root/0/test.txt")
	}
}

func BenchmarkDel(b *testing.B) {
	for n := 0; n < b.N; n++ {
		client.DelFile("/root/0/test.txt")
	}
}

func BenchmarkStat(b *testing.B) {
	for n := 0; n < b.N; n++ {
		client.GetFileStat("/root/0/test.txt")
	}
}

func BenchmarkMkdir(b *testing.B) {
	for n := 0; n < b.N; n++ {
		client.Mkdir("/root", strconv.Itoa(b.N))
	}
}

func BenchmarkRename(b *testing.B) {
	for n := 0; n < b.N; n++ {
		client.ReNameFolder("/root/"+strconv.Itoa(n), "/root/1/"+strconv.Itoa(n))
	}
}

func BenchmarkFiles(b *testing.B) {
	for n := 0; n < b.N; n++ {
		client.GetFiles("/root/1")
	}
}

func BenchmarkFolders(b *testing.B) {
	for n := 0; n < b.N; n++ {
		client.GetCurPathFolder("/root")
	}
}
