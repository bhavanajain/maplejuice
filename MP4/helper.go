package main


import (
    "net"
    "strings"
    "strconv"
    "log"
    "sync"
    "bufio"
    "fmt"
    "time"
    "os"
    "io"
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
    fmt.Printf("%s\n", message)
    padded_message := fillString(message, messageLength)
    conn.Write([]byte(padded_message))
}

func getFileWrapper(sdfsFilename string, localFilename string) {
    fmt.Printf("Inside get file wrapper\n")
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


    master_command := fmt.Sprintf("get %s\n", sdfsFilename)
    fmt.Fprintf(conn, master_command) // get the running 

    fmt.Printf("sent a get request %s to the master\n", master_command) 


    reader := bufio.NewReader(conn)
    reply, err := reader.ReadString('\n')
    if err != nil {
        log.Printf("[ME %d] Could not read reply from master (for get %s)\n", myVid, sdfsFilename)
    }

    conn.Close()    // [NEW]

    reply = reply[:len(reply)-1]
    split_reply := strings.Split(reply, " ")

    if len(split_reply[2]) == 0 {
        log.Printf("invalid file name\n")
        fmt.Printf("invalid file name\n")
        return
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

func PutFileWrapper(localFilename string, sdfsFilename string, conn net.Conn) {
    fmt.Printf("Inside put file wrapper\n")

    _, err := os.Stat(local_dir + localFilename)
    if os.IsNotExist(err) {
        fmt.Printf("Got a put for %s, but the file does not exist\n", localFilename)
        log.Printf("[ME %d] Got a put for %s, but the file does not exist\n", myVid, localFilename)
        return
    }

    // timeout := 20 * time.Second
    // conn, err := net.DialTimeout("tcp", masterIP + ":" + strconv.Itoa(masterPort), timeout)
    // if err != nil {
    //     log.Printf("[ME %d] Unable to connect with the master ip=%s port=%d", myVid, masterIP, masterPort)
    //     return
    // }
    master_command := fmt.Sprintf("put %s %d\n", sdfsFilename, myVid)
    fmt.Fprintf(conn, master_command)

    fmt.Printf("Sent the put request to master\n")

    reader := bufio.NewReader(conn) // Master response
    reply, err := reader.ReadString('\n')
    if err != nil{
        log.Printf(" Can't move forward with put reqest")
        return // free up the user request
    }
    reply = reply[:len(reply)-1]
    fmt.Printf("Master reply for put: %s\n", reply)
    split_reply := strings.Split(reply, " ")
    // Check if it is putreply

    // conn.Close()

    nodeIds_str := strings.Split(split_reply[2], ",")
    nodeIds := []int{}
    for _, node_str := range nodeIds_str {
        node, err := strconv.Atoi(node_str)
        if err != nil {
            panic(err)
            // break // Free up the user 
        }
        nodeIds = append(nodeIds, node)
    }

    var wg sync.WaitGroup
    wg.Add(4)

    doneList = make([]int, 0, 4)

    fmt.Printf("Sending file to %v\n", nodeIds)

    for _, node := range nodeIds {
        go sendFile(node, localFilename, sdfsFilename, &wg, nodeIds)
    }

    wg.Wait()

    doneList_str := list2String(doneList)
    fmt.Printf("Send ack to master")
    sendAcktoMaster("put", myVid, doneList_str, sdfsFilename)

    // elapsed := time.Since(initTime)

    // fmt.Printf("Time taken for put %s\n",elapsed)
}

func sendMapleJuiceAck(action string, srcNode int, taskId int, keysFilename string) {
    ackMessage := fmt.Sprintf("mjAck %s %d %d\n", action, srcNode, taskId)

    timeout := time.Duration(20) * time.Second
    conn, err := net.DialTimeout("tcp", masterIP + ":" + strconv.Itoa(masterPort), timeout)
    if err != nil {
        log.Printf("[ME %d] Unable to connect with the master ip=%s port=%d", myVid, masterIP, masterPort)
        return
    }
    // defer conn.Close()

    fmt.Fprintf(conn, ackMessage)
    simpleSendFile(conn, keysFilename)
    fmt.Printf("Sent all the keys to the master\n")
    conn.Close()
}

func simpleSendFile(conn net.Conn, filename string) {
    f, err := os.Open(filename)
    if err != nil {
        fmt.Printf("Cannot open %s\n", filename)
        return
    }
    defer f.Close()

    fileInfo, err := f.Stat()
    if err != nil {
        fmt.Printf("Can't access file stats for %s\n", filename)
        return
    }

    fileSize := fillString(strconv.FormatInt(fileInfo.Size(), 10), 10)
    fileName := fillString(fileInfo.Name(), 64)

    fmt.Printf("filesize %s\n filename %s\n", fileSize, fileName)

    conn.Write([]byte(fileSize))
    conn.Write([]byte(fileName))

    sendBuffer := make([]byte, BUFFERSIZE)

    for {
        _, err = f.Read(sendBuffer)
        if err == io.EOF {
            break
        }
        conn.Write(sendBuffer)
    }

    fmt.Printf("Completed sending the file %s\n", filename)
    return
}

func simpleRecvFile(conn net.Conn) string {
    bufferFileSize := make([]byte, 10)
    bufferFileName := make([]byte, 64)

    conn.Read(bufferFileSize)
    fileSize, _ := strconv.ParseInt(strings.Trim(string(bufferFileSize), ":"), 10, 64)

    conn.Read(bufferFileName)
    fileName := strings.Trim(string(bufferFileName), ":")

    fmt.Printf("Incoming filesize %d filename %s\n", fileSize, fileName)

    mapleFilePath := maple_dir + fileName
    f, err := os.Create(mapleFilePath)
    if err != nil {
        fmt.Printf("Cannot create file %s\n", mapleFilePath) 
    }

    var receivedBytes int64
    success := true
    for {
        if (fileSize - receivedBytes) < BUFFERSIZE {
            _, err = io.CopyN(f, conn, (fileSize - receivedBytes))
            if err != nil {
                log.Printf("[ME %d] Cannot read file bytes from connection\n", myVid)
                success = false
            }
            break
        }

        _, err = io.CopyN(f, conn, BUFFERSIZE)
        if err != nil {
            log.Printf("[ME %d] Cannot read file bytes from connection\n", myVid)
            success = false
        }
        receivedBytes += BUFFERSIZE
    }
    f.Close()

    if success {
        fmt.Printf("received file %s\n", fileName)
    }

    return fileName
}



