package hdfs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"time"
)

type Raft struct {
	Term int
	// Leader信息
	IsLeader       bool
	LeaderLocation string
	// 日志文件
	TmpLog []*EditLog
	// CommitIndex相关
	CommitIndex int
	MatchIndex  map[string]int
	// heartbeat超时时间
	HeartBeatTicker *time.Ticker
}

type EditLog struct {
	Term        int                 // 附加日志的领导人任期号
	Action      string              // 文件修改行为，putfile、delfile、putdir、deldir
	Path        string              // 文件路径
	IsDir       bool                // 是否是文件夹
	CommitIndex int                 // 日志Index
	File        *File               // put携带的文件及chunk
	DataMap     map[string]string   // rename使用
	NodeMap     map[string][]string // updataNewNode使用
}

type NNHeartBeat struct {
	Term              int        // 附加日志的领导人任期号
	LeaderLocation    string     // 领导人端口号
	PreLogIndex       int        // 当前要附加的日志editLog的上一条的日志索引
	PreLogTerm        int        // 当前要附加的日志editLog的上一条的日志任期号
	EditLog           []*EditLog // 附加日志
	LeaderCommitIndex int        // 当前领导人已经提交的最大的日志索引值
}

type Vote struct {
	Term              int    // 投票任期
	LeaderLocation    string // 领导人地址
	LeaderCommitIndex int    // 当前领导人已经提交的最大的日志索引值
}

func (namenode *NameNode) AddEditLog(action, path string, file *File, isDir bool, dataMap map[string]string, node map[string][]string) bool {
	// 先写入leader日志
	editLog := &EditLog{
		Term:        namenode.Term,
		Action:      action,
		Path:        path,
		IsDir:       isDir,
		CommitIndex: namenode.CommitIndex + 1,
		File:        file,
		DataMap:     dataMap,
		NodeMap:     node,
	}
	namenode.TmpLog = append(namenode.TmpLog, editLog)
	fmt.Printf("receive tmp log=%+v\n", namenode.TmpLog)
	// 通过heartbeat传递日志
	fmt.Println("client heartbeat")
	success := namenode.doHeartBeat()
	// 未成功应用丢弃日志
	if !success && len(namenode.TmpLog) > 0 {
		namenode.TmpLog = namenode.TmpLog[:len(namenode.TmpLog)-1]
	}
	logStr, _ := json.Marshal(namenode.TmpLog)
	fmt.Printf("current tmp log=%+v success=%+v\n", string(logStr), success)
	return success
}

func (namenode *NameNode) doHeartBeat() bool {
	heartBeat := &NNHeartBeat{
		Term:              namenode.Term,
		LeaderLocation:    namenode.Location,
		LeaderCommitIndex: namenode.CommitIndex,
	}
	success := 0
	for _, location := range namenode.NNLocations {
		if location == namenode.Location {
			success++
			continue
		}
		matchIndex := namenode.MatchIndex[location]
		heartBeat.EditLog = namenode.TmpLog[matchIndex:]
		fmt.Printf("matchIndex=%+v tmpLog len=%+v\n", matchIndex, len(namenode.TmpLog))
		if matchIndex == 0 {
			heartBeat.PreLogTerm = 0
			heartBeat.PreLogIndex = 0
		} else {
			heartBeat.PreLogTerm = namenode.TmpLog[matchIndex-1].Term
			heartBeat.PreLogIndex = namenode.TmpLog[matchIndex-1].CommitIndex
		}
		heartbeatStr, _ := json.Marshal(heartBeat)
		fmt.Printf("send heartbeat=%+v\n", string(heartbeatStr))
		// json字符串
		d, _ := json.Marshal(heartBeat)
		// 序列化
		reader := bytes.NewReader(d)
		resp, err := http.Post(location+"/nn_heartbeat", "application/json", reader)
		if err != nil {
			fmt.Println("http post error", err)
			continue
		}
		if resp.StatusCode == http.StatusBadRequest {
			fmt.Println("http bad request")
			continue
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Println("ioutil.ReadAll", err)
			continue
		}
		commitId := 0
		if err := json.Unmarshal(body, &commitId); err != nil {
			fmt.Println("unmarshal nn_heartbeat body error", err)
			continue
		}
		fmt.Printf("receive location=%+v\n commitId=%+v\n", location, commitId)
		if resp.StatusCode == http.StatusNotAcceptable {
			namenode.MatchIndex[location] = commitId
		} else if resp.StatusCode == http.StatusOK {
			namenode.MatchIndex[location] = commitId
			success++
		}
	}
	if success >= (len(namenode.NNLocations))/2+1 && len(namenode.TmpLog) > 0 {
		namenode.CommitIndex = namenode.TmpLog[len(namenode.TmpLog)-1].CommitIndex
		return true
	}
	return false
}

func (namenode *NameNode) doVote() {
	namenode.Term++
	voteNum := 0
	vote := &Vote{
		Term:              namenode.Term,
		LeaderLocation:    namenode.Location,
		LeaderCommitIndex: namenode.CommitIndex,
	}
	fmt.Printf("dovote=%+v time=%+v\n", vote, time.Now())
	for _, location := range namenode.NNLocations {
		if location == namenode.Location {
			voteNum++
			continue
		}
		// json字符串
		d, _ := json.Marshal(vote)
		// 序列化
		reader := bytes.NewReader(d)
		resp, err := http.Post(location+"/vote", "application/json", reader)
		if err != nil {
			fmt.Println("http post error", err)
			continue
		}
		if resp.StatusCode == http.StatusOK {
			voteNum++
		}
	}
	fmt.Printf("voteNum=%+v\n", voteNum)
	if voteNum >= (len(namenode.NNLocations))/2+1 {
		namenode.IsLeader = true
		namenode.LeaderLocation = namenode.Location
		namenode.HeartBeatTicker.Reset(HeartBeatInterval)
		namenode.doHeartBeat()
		fmt.Println("become leader")
	}
}

func (namenode *NameNode) RunHeartBeat() {
	for {
		if namenode == nil {
			fmt.Printf("empty namenode, exit")
			break
		}
		// leader每3秒中从chan t.C 中读取一次, follower每9秒读取一次
		<-namenode.HeartBeatTicker.C
		fmt.Println("Ticker:", time.Now().Format("2006-01-02 15:04:05"))
		if namenode.IsLeader {
			fmt.Println("nn heartbeat")
			namenode.doHeartBeat()
			if !namenode.IsLeader {
				namenode.HeartBeatTicker.Reset(3 * HeartBeatInterval)
			}
		} else {
			fmt.Printf("start vote\n")
			rand.Seed(time.Now().UnixNano())
			// [150-300ms]之间随机选择投票超时时间
			voteTimeout := time.Duration(150 + rand.Intn(150))
			fmt.Printf("voteTimeout=%+v\n", voteTimeout*time.Millisecond)
			namenode.HeartBeatTicker.Reset(voteTimeout * time.Millisecond)
			namenode.doVote()
		}
	}
}

func (namenode *NameNode) ApplyEditLog(log *EditLog) {
	switch log.Action {
	case "put":
		if log.File == nil {
			return
		}
		chunk, _ := json.Marshal(log.File.Chunks)
		fmt.Printf("receive chunk=%+v\n", string(chunk))
		namenode.PutFile(log.File)
		// 将chunk刷入namenode
		for _, chunk := range log.File.Chunks {
			for _, r := range chunk.ReplicaLocationList {
				for j, _ := range namenode.DataNodes {
					if namenode.DataNodes[j].Location == r.ServerLocation {
						for i, c := range namenode.DataNodes[j].ChunkAvail {
							if c == r.ReplicaNum {
								namenode.DataNodes[j].ChunkAvail[i] = namenode.DataNodes[j].ChunkAvail[len(namenode.DataNodes[j].ChunkAvail)-1]
								namenode.DataNodes[j].ChunkAvail = namenode.DataNodes[j].ChunkAvail[:len(namenode.DataNodes[j].ChunkAvail)-1]
								namenode.DataNodes[j].StorageAvail--
								fmt.Printf("change dn location=%+v replicaNum=%+v storage=%+v\n", namenode.DataNodes[j].Location, c, namenode.DataNodes[j].StorageAvail)
								break
							}
						}
						break
					}
				}
			}
		}
	case "delfile":
		for i := 0; i < len(log.File.Chunks); i++ {
			namenode.DelChunk(*log.File, i)
		}
	case "reFolderName":
		namenode.NameSpace.ReNameFolderName(log.DataMap["preFolder"], log.DataMap["reNameFolder"])
	case "mkdir":
		namenode.NameSpace.CreateFolder(log.DataMap["curPath"], log.DataMap["folderName"])
	case "updataNewNode":
		namenode.UpdateNewNode(log.NodeMap)
	}
}
