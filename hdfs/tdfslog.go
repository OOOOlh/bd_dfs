package hdfs

import (
	"log"
)

var (
	TDFSLogger *log.Logger
)

func init(){
	TDFSLogger = LogInit("D:/go/workspace/src/tinydfs/TDFSLog.txt", "TDFS Log: ")
}

func LogInit(logFilename string, prefix string) (TDFSLogger *log.Logger){
	logFile := OpenFile(logFilename)
	// fmt.Println(logFile)
	TDFSLogger = log.New(logFile, prefix, log.Ldate|log.Ltime|log.Lshortfile)
	return TDFSLogger
}