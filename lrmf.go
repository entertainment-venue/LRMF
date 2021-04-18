package LRMF

import (
	"log"
	"os"
)

// StdLogger is used to log error messages.
type StdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

var Logger StdLogger = log.New(os.Stdout, "[LRMF] ", log.LstdFlags|log.Lshortfile)
