package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

const (
	TimeFormat = "2006-01-02T15:04:05.000"

	// Context keys
	CtxRequestID = "request_id"
	CtxTenantID  = "tenant_id"
	CtxThread    = "thread"
)

type LogFields struct {
	RequestID       string
	TenantID        string
	Thread          string
	Class           string
	MethodName      string
	Version         string
	ErrorCode       string
	OriginatingBIID string
	BusinessIDs     map[string]interface{}
	TraceID         string
	SpanID          string
	CustomFields    map[string]interface{}
}

type StructuredLogger struct {
	logger *zap.Logger
}

func NewStructuredLogger(level string) (*StructuredLogger, error) {
	baseLogger, err := newZapLogger(level)
	if err != nil {
		return nil, err
	}
	return &StructuredLogger{logger: baseLogger}, nil
}

func NewStructuredLoggerFromZap(baseLogger *zap.Logger) *StructuredLogger {
	return &StructuredLogger{logger: baseLogger}
}

func NewZapLogger(level string) (*zap.Logger, error) {
	return newZapLogger(level)
}

type strictJSONEncoder struct {
	zapcore.Encoder
}

type jsonPair struct {
	key   string
	value interface{}
}

func newZapLogger(level string) (*zap.Logger, error) {
	logLevel := zap.InfoLevel
	if err := logLevel.UnmarshalText([]byte(level)); err != nil {
		logLevel = zap.InfoLevel
	}

	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "",
		CallerKey:      "caller",
		FunctionKey:    "",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     func(t time.Time, enc zapcore.PrimitiveArrayEncoder) { enc.AppendString(t.Format(TimeFormat)) },
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	encoder := strictJSONEncoder{zapcore.NewJSONEncoder(encoderConfig)}
	core := zapcore.NewCore(encoder, zapcore.Lock(os.Stdout), zap.NewAtomicLevelAt(logLevel))
	logger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zap.ErrorLevel))
	return logger, nil
}

func (e strictJSONEncoder) Clone() zapcore.Encoder {
	return strictJSONEncoder{e.Encoder.Clone()}
}

func (e strictJSONEncoder) EncodeEntry(ent zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	buf, err := e.Encoder.EncodeEntry(ent, fields)
	if err != nil {
		return nil, err
	}

	var data map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &data); err != nil {
		return buf, err
	}

	ordered, err := marshalStrictJSON(ent, data)
	if err != nil {
		return buf, err
	}

	out := buffer.NewPool().Get()
	out.AppendString(string(ordered))
	return out, nil
}

func marshalStrictJSON(ent zapcore.Entry, data map[string]interface{}) ([]byte, error) {
	if data == nil {
		data = make(map[string]interface{})
	}

	data["time"] = ent.Time.Format(TimeFormat)
	data["level"] = strings.ToUpper(ent.Level.String())

	if _, ok := data["request_id"]; !ok {
		data["request_id"] = "-"
	}
	if _, ok := data["tenant_id"]; !ok {
		data["tenant_id"] = "-"
	}
	if _, ok := data["thread"]; !ok {
		data["thread"] = "-"
	}

	if _, ok := data["class"]; !ok {
		if callerRaw, ok := data["caller"].(string); ok && callerRaw != "" {
			parts := strings.Split(callerRaw, ":")
			path := parts[0]
			path = strings.ReplaceAll(path, "\\", "/")
			segs := strings.Split(path, "/")
			if len(segs) >= 2 {
				file := segs[len(segs)-1]
				parent := segs[len(segs)-2]
				name := strings.TrimSuffix(file, ".go")
				if name != "" {
					if len(name) > 1 {
						name = strings.ToUpper(name[:1]) + name[1:]
					} else {
						name = strings.ToUpper(name)
					}
					data["class"] = fmt.Sprintf("%s.%s", parent, name)
				} else {
					data["class"] = parent
				}
			} else if len(segs) == 1 {
				name := strings.TrimSuffix(segs[0], ".go")
				if name != "" {
					if len(name) > 1 {
						name = strings.ToUpper(name[:1]) + name[1:]
					} else {
						name = strings.ToUpper(name)
					}
					data["class"] = name
				} else {
					data["class"] = "-"
				}
			} else {
				data["class"] = "-"
			}
		} else {
			data["class"] = "-"
		}
	}
	data["msg"] = ent.Message

	orderedKeys := []string{"time", "level", "request_id", "tenant_id", "thread", "class", "caller"}
	optionalKeys := []string{"method", "version", "error_code", "originating_bi_id", "business_identifiers", "traceId", "spanId"}
	for _, key := range optionalKeys {
		if _, ok := data[key]; ok {
			orderedKeys = append(orderedKeys, key)
		}
	}

	remaining := make([]string, 0, len(data))
	for key := range data {
		if key == "time" || key == "level" || key == "request_id" || key == "tenant_id" || key == "thread" || key == "class" || key == "caller" || key == "msg" {
			continue
		}
		skip := false
		for _, optionalKey := range optionalKeys {
			if key == optionalKey {
				skip = true
				break
			}
		}
		if !skip {
			remaining = append(remaining, key)
		}
	}
	sort.Strings(remaining)
	orderedKeys = append(orderedKeys, remaining...)
	orderedKeys = append(orderedKeys, "msg")

	pairs := make([]jsonPair, 0, len(orderedKeys))
	for _, key := range orderedKeys {
		if value, ok := data[key]; ok {
			pairs = append(pairs, jsonPair{key: key, value: value})
		}
	}

	return marshalOrderedJSON(pairs)
}

func marshalOrderedJSON(pairs []jsonPair) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, pair := range pairs {
		if i > 0 {
			buf.WriteByte(',')
		}
		keyBytes, err := json.Marshal(pair.key)
		if err != nil {
			return nil, err
		}
		buf.Write(keyBytes)
		buf.WriteByte(':')
		valueBytes, err := json.Marshal(pair.value)
		if err != nil {
			return nil, err
		}
		buf.Write(valueBytes)
	}
	buf.WriteByte('}')
	buf.WriteByte('\n')
	return buf.Bytes(), nil
}

func fieldsToZapFields(fields *LogFields, keysAndValues ...interface{}) []zap.Field {
	var zapFields []zap.Field
	if fields != nil {
		zapFields = append(zapFields,
			zap.String("request_id", defaultString(fields.RequestID)),
			zap.String("tenant_id", defaultString(fields.TenantID)),
			zap.String("thread", defaultString(fields.Thread)),
			zap.String("class", defaultString(fields.Class)),
		)
		if fields.MethodName != "" {
			zapFields = append(zapFields, zap.String("method", fields.MethodName))
		}
		if fields.Version != "" {
			zapFields = append(zapFields, zap.String("version", fields.Version))
		}
		if fields.ErrorCode != "" {
			zapFields = append(zapFields, zap.String("error_code", fields.ErrorCode))
		}
		if fields.OriginatingBIID != "" {
			zapFields = append(zapFields, zap.String("originating_bi_id", fields.OriginatingBIID))
		}
		if len(fields.BusinessIDs) > 0 {
			zapFields = append(zapFields, zap.Any("business_identifiers", fields.BusinessIDs))
		}
		if fields.TraceID != "" {
			zapFields = append(zapFields, zap.String("traceId", fields.TraceID))
		}
		if fields.SpanID != "" {
			zapFields = append(zapFields, zap.String("spanId", fields.SpanID))
		}
		for k, v := range fields.CustomFields {
			zapFields = append(zapFields, zap.Any(k, v))
		}
	}
	for i := 0; i < len(keysAndValues); i += 2 {
		if i+1 < len(keysAndValues) {
			key, ok := keysAndValues[i].(string)
			if !ok {
				key = fmt.Sprint(keysAndValues[i])
			}
			zapFields = append(zapFields, zap.Any(key, keysAndValues[i+1]))
		}
	}
	return zapFields
}

func defaultString(value string) string {
	if value == "" {
		return "-"
	}
	return value
}

func (sl *StructuredLogger) Info(msg string, fields *LogFields, keysAndValues ...interface{}) {
	sl.logger.Info(msg, fieldsToZapFields(fields, keysAndValues...)...)
}

func (sl *StructuredLogger) Debug(msg string, fields *LogFields, keysAndValues ...interface{}) {
	sl.logger.Debug(msg, fieldsToZapFields(fields, keysAndValues...)...)
}

func (sl *StructuredLogger) Warn(msg string, fields *LogFields, keysAndValues ...interface{}) {
	sl.logger.Warn(msg, fieldsToZapFields(fields, keysAndValues...)...)
}

func (sl *StructuredLogger) Error(msg string, fields *LogFields, err error, keysAndValues ...interface{}) {
	zapFields := fieldsToZapFields(fields, keysAndValues...)
	if err != nil {
		zapFields = append(zapFields, zap.String("error", err.Error()))
	}
	sl.logger.Error(msg, zapFields...)
}

func (sl *StructuredLogger) Fatal(msg string, fields *LogFields, keysAndValues ...interface{}) {
	sl.logger.Fatal(msg, fieldsToZapFields(fields, keysAndValues...)...)
}

func (sl *StructuredLogger) WithContext(ctx context.Context, fields *LogFields) *LogFields {
	if fields == nil {
		fields = NewLogFields()
	} else {
		cloned := *fields
		cloned.CustomFields = make(map[string]interface{}, len(fields.CustomFields))
		for k, v := range fields.CustomFields {
			cloned.CustomFields[k] = v
		}
		cloned.BusinessIDs = make(map[string]interface{}, len(fields.BusinessIDs))
		for k, v := range fields.BusinessIDs {
			cloned.BusinessIDs[k] = v
		}
		fields = &cloned
	}

	if requestID := ctx.Value(CtxRequestID); requestID != nil {
		if id, ok := requestID.(string); ok && fields.RequestID == "" {
			fields.RequestID = id
		}
	}

	if tenantID := ctx.Value(CtxTenantID); tenantID != nil {
		if id, ok := tenantID.(string); ok && fields.TenantID == "" {
			fields.TenantID = id
		}
	}

	if thread := ctx.Value(CtxThread); thread != nil {
		if t, ok := thread.(string); ok && fields.Thread == "" {
			fields.Thread = t
		}
	}

	return fields
}

func (sl *StructuredLogger) Sync() error {
	err := sl.logger.Sync()

	if err != nil &&
		strings.Contains(err.Error(), "invalid argument") {
		return nil
	}
	return err
}

func NewLogFields() *LogFields {
	return &LogFields{
		CustomFields: make(map[string]interface{}),
		BusinessIDs:  make(map[string]interface{}),
	}
}

func (lf *LogFields) WithRequestID(id string) *LogFields {
	lf.RequestID = id
	return lf
}

func (lf *LogFields) WithTenantID(id string) *LogFields {
	lf.TenantID = id
	return lf
}

func (lf *LogFields) WithThread(thread string) *LogFields {
	lf.Thread = thread
	return lf
}

func (lf *LogFields) WithMethod(method string) *LogFields {
	lf.MethodName = method
	return lf
}

func (lf *LogFields) WithTraceID(id string) *LogFields {
	lf.TraceID = id
	return lf
}

func (lf *LogFields) WithSpanID(id string) *LogFields {
	lf.SpanID = id
	return lf
}

func (lf *LogFields) WithClass(class string) *LogFields {
	lf.Class = class
	return lf
}

func (lf *LogFields) AddCustomField(key string, value interface{}) *LogFields {
	lf.CustomFields[key] = value
	return lf
}
