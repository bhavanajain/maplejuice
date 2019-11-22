package main


import (
    // "os"
    "net"
    "strings"
    "strconv"
    "log"
    // "sync"
    "bufio"
    // "io"
    "fmt"
    "time"
    // "io/ioutil"
    // "math/rand"
    // "errors"
    // "math"
)

func sendMapleInfo(nodeId int, mapleId int, sdfsMapleExe string, inputFile string, sdfsInterPrefix string) {
	timeout := 20 * time.Second

    ip := memberMap[nodeId].ip
    port := fileTransferPort

    conn, err := net.DialTimeout("tcp", ip + ":" + strconv.Itoa(port), timeout) 
    if err != nil {
        log.Printf("[ME %d] Unable to dial a connection to %d (to send maple task for %s)\n", myVid, nodeId, sdfsMapleExe)
        return
    }
    defer conn.Close()

    message := fmt.Sprintf("runmaple %d %s %s %s", mapleId, sdfsMapleExe, inputFile, sdfsInterPrefix)
    padded_message := fillString(message, messageLength)
    conn.Write([]byte(padded_message))
}

func getFileWrapper(sdfsFilename string, localFilename string) {
    initTime := time.Now()
    // sdfsFilename := split_command[1]
    // localFilename := split_command[2]
    // Create a connection to main to ask for the file
	timeout := 20 * time.Second
    
    conn, err := net.DialTimeout("tcp", masterIP + ":" + strconv.Itoa(masterPort), timeout)
    if err != nil {
        log.Printf("[ME %d] Unable to connect with the master ip=%s port=%d", myVid, masterIP, masterPort)
        return
    }
    // defer conn.Close() // Don't defer
    master_command := fmt.Sprintf("get %s\n", sdfsFilename)
    fmt.Fprintf(conn, master_command) // get the running 

    reader := bufio.NewReader(conn)
    reply, err := reader.ReadString('\n')
    if err != nil {
        log.Printf("[ME %d] Could not read reply from master (for get %s)\n", myVid, sdfsFilename)
    }

    conn.Close()    // [NEW]

    reply = reply[:len(reply)-1]
    split_reply := strings.Split(reply, " ")

    if len(split_reply[2]) == 0 {
        // fmt.Printf("invalid file name\n")
        log.Printf("invalid file name\n")
        fmt.Printf("invalid file name\n")
        return // with some error msgs
    }

    nodeIds_str := strings.Split(split_reply[2], ",")

    // fmt.Printf("nodestr: %v %d\n", nodeIds_str, len(nodeIds_str))

    nodeIds := []int{}
    for _, node_str := range nodeIds_str {
        node, err := strconv.Atoi(node_str)
        if err != nil {
            panic(err)
        }
        nodeIds = append(nodeIds, node)
    }

    success := false
    for _, node := range(nodeIds) {
        success = getFile(node, sdfsFilename, localFilename)
        if success {
            fmt.Printf("File received!!! %s %s\n",sdfsFilename,localFilename)
            break
        }
    }

    elapsed := time.Since(initTime)

    if !success {
        fmt.Printf("[ME %d] Could not fetch %s shared to %s local\n", myVid, sdfsFilename, localFilename)
    }

    fmt.Printf("Time taken for get %s\n", elapsed)
    return

}