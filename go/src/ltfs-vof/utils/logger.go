package utils

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"
)

type Logger struct {
	Filename string
}

func NewLogger(filename string, cleanup bool) *Logger {
	// if cleanup create or clear the log file
	if cleanup {
		// remove the file if it exists
		os.Remove(filename)
	}
	// see if file exists
	_, err := os.Stat(filename)
	if err != nil {
		// create the file
		os.Create(filename)
	}
	return &Logger{Filename: filename}
}
func (l *Logger) Event(message ...any) {
	// open the file in append mode
	f, err := os.OpenFile(l.Filename, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	// write the header to the file
	if _, err := fmt.Fprintf(f, " %s\n\tEvent: ", l.getHeader()); err != nil {
		fmt.Println("Error writing event to log file:", err)
	}
	// write the message to the file
	if _, err := fmt.Fprintln(f, message); err != nil {
		fmt.Println("Error writing event to log file:", err)
	}
}
func (l *Logger) Fatal(message ...any) {
	// open the file in append mode
	f, err := os.OpenFile(l.Filename, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	// write the header to the file
	if _, err := fmt.Fprintf(f, "%s \n\tFatal: ", l.getHeader()); err != nil {
		fmt.Println("Error writing event to log file:", err)
	}
	// write the message to the file
	if _, err := fmt.Fprintln(f, message); err != nil {
		fmt.Println("Error writing fatal to log file:", err)
	}
	os.Exit(1)
}
func (l *Logger) getHeader() string {
	// runtime.Caller returns the program counter, file name, line number, and a boolean indicating success.
	pc, file, line, ok := runtime.Caller(2) // +2 to account for getCallerInfo and logger function
	if !ok {
		return "unknown"
	}

	// runtime.FuncForPC returns a *runtime.Func object for the given program counter.
	f := runtime.FuncForPC(pc)
	if f == nil {
		return "unknown"
	}

	// Extract the function name from the fully qualified name.
	// For example, "main.myFunction" becomes "myFunction".
	funcName := f.Name()
	lastSlash := strings.LastIndex(funcName, "/")
	if lastSlash != -1 {
		funcName = funcName[lastSlash+1:]
	}
	lastDot := strings.LastIndex(funcName, ".")
	if lastDot != -1 {
		funcName = funcName[lastDot+1:]
	}
	return fmt.Sprintf("%s:Func %s Line %d:  ", time.Now().Format("2006-01-02 15:04:05"), file, line)
}
