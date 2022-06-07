package logger

import (
	"fmt"
	"github.com/topfreegames/pitaya/v2/logger/interfaces"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type ZapPitayaLogger struct {
	zapBase  *zap.Logger
	zapSugar *zap.SugaredLogger
}

func (z *ZapPitayaLogger) Fatal(format ...interface{}) {
	z.zapSugar.Fatal(format...)
}

func (z *ZapPitayaLogger) Fatalf(format string, args ...interface{}) {
	z.zapSugar.Fatalf(format, args...)
}

func (z *ZapPitayaLogger) Fatalln(args ...interface{}) {
	msg := fmt.Sprintln(args...)
	z.zapSugar.Fatal(msg[:len(msg)-1])
}

func (z *ZapPitayaLogger) Debug(args ...interface{}) {
	z.zapSugar.Debug(args...)
}

func (z *ZapPitayaLogger) Debugf(format string, args ...interface{}) {
	z.zapSugar.Debugf(format, args...)
}

func (z *ZapPitayaLogger) Debugln(args ...interface{}) {
	msg := fmt.Sprintln(args...)
	z.zapSugar.Debug(msg[:len(msg)-1])
}

func (z *ZapPitayaLogger) Error(args ...interface{}) {
	z.zapSugar.Error(args...)
}

func (z *ZapPitayaLogger) Errorf(format string, args ...interface{}) {
	z.zapSugar.Errorf(format, args...)
}

func (z *ZapPitayaLogger) Errorln(args ...interface{}) {
	msg := fmt.Sprintln(args...)
	z.zapSugar.Error(msg[:len(msg)-1])
}

func (z *ZapPitayaLogger) Info(args ...interface{}) {
	z.zapSugar.Info(args...)
}

func (z *ZapPitayaLogger) Infof(format string, args ...interface{}) {
	z.zapSugar.Infof(format, args...)
}

func (z *ZapPitayaLogger) Infoln(args ...interface{}) {
	msg := fmt.Sprintln(args...)
	z.zapSugar.Info(msg[:len(msg)-1])
}

func (z *ZapPitayaLogger) Warn(args ...interface{}) {
	z.zapSugar.Warn(args...)
}

func (z *ZapPitayaLogger) Warnf(format string, args ...interface{}) {
	z.zapSugar.Warnf(format, args...)
}

func (z *ZapPitayaLogger) Warnln(args ...interface{}) {
	msg := fmt.Sprintln(args...)
	z.zapSugar.Warn(msg[:len(msg)-1])
}

func (z *ZapPitayaLogger) Panic(args ...interface{}) {
	z.zapSugar.Panic(args...)
}

func (z *ZapPitayaLogger) Panicf(format string, args ...interface{}) {
	z.zapSugar.Panicf(format, args...)
}

func (z *ZapPitayaLogger) Panicln(args ...interface{}) {
	msg := fmt.Sprintln(args...)
	z.zapSugar.Panic(msg[:len(msg)-1])
}

func (z *ZapPitayaLogger) WithFields(fields map[string]interface{}) interfaces.Logger {
	var args []interface{}
	for k, v := range fields {
		args = append(args, transInterfaceToFieldNew(k, v))
	}
	return &ZapPitayaLogger{zapSugar: z.zapBase.Sugar().With(args)}
}

func (z *ZapPitayaLogger) WithField(key string, value interface{}) interfaces.Logger {
	var args []interface{}
	args = append(args, transInterfaceToFieldNew(key, value))
	return &ZapPitayaLogger{zapSugar: z.zapBase.Sugar().With(args)}
}

func (z *ZapPitayaLogger) WithError(err error) interfaces.Logger {
	var args []interface{}
	args = append(args, transInterfaceToFieldNew("error", err))
	return &ZapPitayaLogger{zapSugar: z.zapBase.Sugar().With(args)}
}

func transInterfaceToFieldNew(key string, input interface{}) (result zap.Field) {
	result = zap.Field{Key: key, Interface: input}
	rt := reflect.TypeOf(input)
	if rt.Kind() == reflect.String {
		result.Type = zapcore.StringType
		result.String = fmt.Sprintf("%s", input)
	} else if rt.Kind() == reflect.Float64 {
		result.Type = zapcore.Float64Type
	} else if rt.Kind() == reflect.Int64 {
		result.Type = zapcore.Int64Type
		result.Integer, _ = strconv.ParseInt(fmt.Sprintf("%d", input), 0, 64)
	} else if strings.Contains(rt.String(), "errors.errorString") {
		result.Type = zapcore.ErrorType
	} else {
		result.Type = zapcore.StringType
		result.String = fmt.Sprintf("%v", input)
	}
	return
}

func InitZapPitayaLogger(config *LogConfig) *ZapPitayaLogger {
	config.LogLevel = strings.ToLower(config.LogLevel)
	var level int
	for i, v := range levelStringList {
		if v == config.LogLevel {
			level = i - 1
			break
		}
	}
	if config.LogLevel == "fatal" {
		level = 5
	}
	zapLevel := zap.NewAtomicLevel()
	zapLevel.SetLevel(zapcore.Level(level))
	hook := lumberjack.Logger{
		Filename:   config.FilePath,
		MaxSize:    config.ArchiveMaxSize,
		MaxBackups: config.ArchiveMaxBackup,
		MaxAge:     config.ArchiveMaxDay,
		Compress:   config.Compress,
	}
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:       "time",
		LevelKey:      "level",
		NameKey:       "logger",
		CallerKey:     "caller",
		MessageKey:    "msg",
		StacktraceKey: "stacktrace",
		LineEnding:    zapcore.DefaultLineEnding,
		EncodeLevel:   zapcore.CapitalLevelEncoder,
		EncodeTime: func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(t.Format("2006-01-02 15:04:05.000"))
		},
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
	var zCore zapcore.Core
	if !config.FormatJson {
		zCore = zapcore.NewCore(zapcore.NewConsoleEncoder(encoderConfig), zapcore.NewMultiWriteSyncer(zapcore.AddSync(&hook)), zapLevel)
	} else {
		zCore = zapcore.NewCore(zapcore.NewJSONEncoder(encoderConfig), zapcore.NewMultiWriteSyncer(zapcore.AddSync(&hook)), zapLevel)
	}
	zapLogger := zap.New(zCore, zap.AddCaller(), zap.Development())
	zapLogger.Info("Success init zap log !!")
	return &ZapPitayaLogger{zapBase: zapLogger, zapSugar: zapLogger.Sugar()}
}
