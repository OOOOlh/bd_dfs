package hdfs

import (
	"testing"
)

func getNodes() *Folder {
	return &Folder{
		"root",
		[]*Folder{},
		[]*File{{
			"data",
			1024,
			[]FileChunk{},
			0,
			"",
		}},
	}
}

func TestGetFileList(t *testing.T) {
	Nodes := getNodes()
	FileList := Nodes.GetFileList("/root")
	t.Log(FileList)
	if FileList != nil {
		t.Log(len(FileList))
	}
}

func TestGetFile(t *testing.T) {
	Nodes := getNodes()
	File := Nodes.GetFileNode("/root/data.txt")
	if File != nil {
		t.Log(File.Name)
	}
}

//func TestFunc(t *testing.T) {
//	x := "/root/teset"
//	fmt.Println(strings.Split(x, "/"))
//	fmt.Println(len(strings.Split(x, "/")))
//}
