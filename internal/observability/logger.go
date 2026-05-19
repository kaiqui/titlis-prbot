package observability

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

func parseLevel(s string) Level {
	switch s {
	case "debug":
		return LevelDebug
	case "warn":
		return LevelWarn
	case "error":
		return LevelError
	default:
		return LevelInfo
	}
}

type Logger struct {
	mu     sync.Mutex
	w      io.Writer
	level  Level
	format string
}

func NewLogger(level, format string) *Logger {
	return &Logger{w: os.Stdout, level: parseLevel(level), format: format}
}

func NewLoggerTo(w io.Writer, level, format string) *Logger {
	return &Logger{w: w, level: parseLevel(level), format: format}
}

func (l *Logger) log(lvl Level, msg string, kv map[string]any) {
	if lvl < l.level {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.format == "json" {
		entry := map[string]any{
			"ts":    time.Now().UTC().Format(time.RFC3339Nano),
			"level": levelString(lvl),
			"msg":   msg,
		}
		for k, v := range kv {
			entry[k] = v
		}
		_ = json.NewEncoder(l.w).Encode(entry)
		return
	}
	fmt.Fprintf(l.w, "%s [%s] %s", time.Now().UTC().Format(time.RFC3339), levelString(lvl), msg)
	for k, v := range kv {
		fmt.Fprintf(l.w, " %s=%v", k, v)
	}
	fmt.Fprintln(l.w)
}

func levelString(l Level) string {
	switch l {
	case LevelDebug:
		return "debug"
	case LevelWarn:
		return "warn"
	case LevelError:
		return "error"
	default:
		return "info"
	}
}

func (l *Logger) Info(msg string, kv ...any)  { l.log(LevelInfo, msg, kvMap(kv)) }
func (l *Logger) Warn(msg string, kv ...any)  { l.log(LevelWarn, msg, kvMap(kv)) }
func (l *Logger) Error(msg string, kv ...any) { l.log(LevelError, msg, kvMap(kv)) }
func (l *Logger) Debug(msg string, kv ...any) { l.log(LevelDebug, msg, kvMap(kv)) }

func kvMap(kv []any) map[string]any {
	out := make(map[string]any, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		k, ok := kv[i].(string)
		if !ok {
			continue
		}
		out[k] = kv[i+1]
	}
	return out
}
