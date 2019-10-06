package main

import (
	"fmt"
	"time"
)

func printAlphabet() {
	time.Sleep(5 * time.Second)
	alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	for _, c := range(alphabet) {
		fmt.Println(c)
	}
}

func catchSignal() {
	
}

func main() {
	signal_chan := make(chan os.Signal, 1)
	signal.Notify(syscall.SIGQUIT)
	

	
}