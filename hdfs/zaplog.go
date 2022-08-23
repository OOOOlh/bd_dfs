package hdfs

import (
	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var sugarLogger *zap.SugaredLogger

func init(){
	InitLogger()
}

func InitLogger() {
	writeSyncer := getLogWriter()
  encoder := getEncoder()
  core := zapcore.NewCore(encoder, writeSyncer, zapcore.DebugLevel)

  logger := zap.New(core, zap.AddCaller())
  sugarLogger = logger.Sugar()
}

func getEncoder() zapcore.Encoder {
	encoderConfig := zap.NewProductionEncoderConfig()
  encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
  encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
  return zapcore.NewConsoleEncoder(encoderConfig)
}

func getLogWriter() zapcore.WriteSyncer {
	lumberJackLogger := &lumberjack.Logger{
		Filename:   "./zap.log",
		MaxSize:    1,
		MaxBackups: 5,
		MaxAge:     30,
		Compress:   false,
}
	return zapcore.AddSync(lumberJackLogger)
}