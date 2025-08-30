package utils

import (
	"fmt"
	"os"
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
	if _, err := fmt.Fprintf(f, "%s: Event: ", l.getTime()); err != nil {
		fmt.Println("Error writing event to log file:", err)
	}
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
	if _, err := fmt.Fprintf(f, "%s: Fatal: ", l.getTime()); err != nil {
		fmt.Println("Error writing event to log file:", err)
	}
	// write the message to the file
	if _, err := fmt.Fprintln(f, message); err != nil {
		fmt.Println("Error writing fatal to log file:", err)
	}
	os.Exit(1)
}
func (l *Logger) getTime() string {
	// get the current time in the format YYYY-MM-DD HH:MM:SS
	return time.Now().Format("2006-01-02 15:04:05")
}
