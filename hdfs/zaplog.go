package hdfs

import (
	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var sugarLogger *zap.SugaredLogger

func init(){
	InitLogger("./zap.log")
}

func InitLogger(dir string) *zap.SugaredLogger{
	writeSyncer := getLogWriter(dir)
  encoder := getEncoder()
  core := zapcore.NewCore(encoder, writeSyncer, zapcore.DebugLevel)

  logger := zap.New(core, zap.AddCaller())
  sugarLogger = logger.Sugar()
	return sugarLogger
}

func getEncoder() zapcore.Encoder {
	encoderConfig := zap.NewProductionEncoderConfig()
  encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
  encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
  return zapcore.NewConsoleEncoder(encoderConfig)
}

func getLogWriter(dir string) zapcore.WriteSyncer {
	lumberJackLogger := &lumberjack.Logger{
		Filename:   dir,
		MaxSize:    1,
		MaxBackups: 5,
		MaxAge:     30,
		Compress:   false,
}
	return zapcore.AddSync(lumberJackLogger)
}