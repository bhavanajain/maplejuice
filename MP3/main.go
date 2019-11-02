package main

import (
	"os"
	"flag"
	"sync"
	"time"
	"os/signal"
	// "io"
	"log"
	"fmt"
	"syscall"
)


func distributedFileSystem() {
    os.RemoveAll(shared_dir)
    os.MkdirAll(shared_dir, 0777)
    os.RemoveAll(temp_dir)
    os.MkdirAll(temp_dir, 0777)

    myIP := getmyIP()
    if myIP == masterIP {
        go listenMasterRequests()
    } else {
        go listenFileTransferPort()
        go scanCommands()
    }

    go ReplicateFile()

}

func main() {
	logFile := flag.String("logfile", "", "path to the log file")
	flag.Parse()

	f, err := os.Create(*logFile)
	if err != nil {
		log.Fatalf("Error opening the log file: %v", err)
	}
	defer f.Close()
	// mw := io.MultiWriter(os.Stdout, f)
	log.SetOutput(f)

	var wg sync.WaitGroup
	wg.Add(1)

	go membership(&wg)
	go distributedFileSystem()

	wg.Wait()
	return
}


func membership(wg *sync.WaitGroup) {
	
	myIP = getmyIP()
	log.Printf("%s", myIP)

	if myIP == introducer {
		myVid = 0
		var node MemberNode
		node.ip = myIP
		node.timestamp = time.Now().Unix()
		node.alive = true
		memberMap[0] = &node
	}

	go sendHeartbeat()
	go receiveHeartbeat()
	go checkChildren()

	go listenOtherPort()

	time.Sleep(time.Duration(introPingPeriod) * time.Second)
	
	if myIP == introducer {
		// there should be a delay here - depending on how frequently the introducer is being pinged
		// if the system already exists in some form, the introducer shouldn't accept join requests until it knows what the maxID is 
		go completeJoinRequests()
		go garbageCollection()
		// this garbage collection can occur concurrent to the addToDead list

	} else{
		sendJoinRequest()
	}
	go updateFingerTable()

	sigs := make(chan os.Signal, 1)

	signal.Notify(sigs, syscall.SIGQUIT)

	go func() {
		sig := <-sigs
		switch sig {
		case syscall.SIGQUIT:
			leave_time := time.Now().Unix()
			message := fmt.Sprintf("LEAVE,%d,%d", myVid, leave_time)
			disseminate(message)
			
			wg.Done()
		}
	}()
}

