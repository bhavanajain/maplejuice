package main

import (
    "os"
    "net"
    "strings"
    "strconv"
    "log"
    "sync"
    "bufio"
    "io"
    "fmt"
    "time"
    "io/ioutil"
)

var head = 0
const BUFFERSIZE = 1024

type fileData struct {
    timestamp int64
    nodeIds []int
}

type nodeData struct {
    fileNames []string
}

var local_dir = "local/"
var shared_dir = "shared/"
var temp_dir = "temp/"

var masterIP = "172.22.152.106"
var masterPort = 8084
var fileTransferPort = 8085

// var myVid int

func fillString(retunString string, toLength int) string {
    for {
        lengtString := len(retunString)
        if lengtString < toLength {
            retunString = retunString + ":"
            continue
        }
        break
    }
    return retunString
}


func getRandomNodes() []int {
    nodes := []int{1, 2, 3, 4}
    return nodes
}

func listenFileTransferPort() {
    // This port serves file transfers
    // message type: getfile, putfile, deletefile

    ln, err := net.Listen("tcp", ":" + strconv.Itoa(fileTransferPort))
    if err != nil {
        log.Printf("[ME %d] Cannot listen on file transfer port %d\n", myVid, fileTransferPort)
    }
    log.Printf("[ME %d] Started listening on file transfer port %d\n", myVid, fileTransferPort)

    for {
        conn, _ := ln.Accept()
        log.Println("[ME %d] Accepted a new connection", myVid)     
        bufferMessage := make([]byte, 64)
        // conn_reader := bufio.NewReader(conn)
        conn.Read(bufferMessage)
        message := strings.Trim(string(bufferMessage), ":")
        // message, _ := conn_reader.ReadString('\n')
        // message = message[:len(message)-1]
        fmt.Printf("[ME %d] Received a new message %s\n", myVid, message)

        split_message := strings.Split(message, " ")
        message_type := split_message[0]

        switch message_type {
            case "getfile":
                sdfsFilename := split_message[1]
                f, err := os.Open(shared_dir + "/" + sdfsFilename)
                if err != nil {
                    log.Printf("[ME %d] Can't open file %s\n", myVid, sdfsFilename)
                    break
                }

                fileInfo, err := f.Stat()
                if err != nil {
                    log.Printf("[ME %D] Can't access file stats %s\n", myVid, sdfsFilename)
                    return
                }

                fileSize := fillString(strconv.FormatInt(fileInfo.Size(), 10), 10)
                // fileName := fillString(fileInfo.Name(), 64)
                log.Printf("[ME %d] filesize %s", myVid, fileSize)
                conn.Write([]byte(fileSize))
                // conn.Write([]byte(fileName))

                sendBuffer := make([]byte, BUFFERSIZE)

                success := true
                for {
                    _, err = f.Read(sendBuffer)
                    if err != nil {
                        if err == io.EOF {
                            break
                        } else {
                            success = false
                            log.Printf("[ME %d] Error while reading file %s\n", myVid, sdfsFilename)
                            break
                        }
                    }
                    
                    _, err := conn.Write(sendBuffer)
                    if err != nil {
                        success = false
                        log.Printf("[ME %d] Error while sending file %s\n", myVid, sdfsFilename)
                    }
                }
                if success {
                    log.Printf("[ME %d] Successfully sent the complete file %s\n", myVid, sdfsFilename)
                } else {
                    log.Printf("[ME %d] Could not send the complete file %s\n", myVid, sdfsFilename)
                    break
                }

                f.Close()


            case "putfile":
                sdfsFilename := split_message[1]

                fmt.Printf("%s %s\n", message_type, sdfsFilename)

                bufferFileSize := make([]byte, 10)
                conn.Read(bufferFileSize)

                fmt.Printf("%s\n", string(bufferFileSize))
                fileSize, _ := strconv.ParseInt(strings.Trim(string(bufferFileSize), ":"), 10, 64)
                fmt.Printf("%d\n", fileSize)

                log.Println("[ME %d] Incoming filesize %d\n", myVid, fileSize)

                f, err := os.Create(temp_dir + sdfsFilename)
                if err != nil {
                    log.Println("[ME %d] Cannot create file %s\n", myVid, sdfsFilename) 
                }

                var receivedBytes int64
                success := true
                for {
                    if (fileSize - receivedBytes) < BUFFERSIZE {
                        _, err = io.CopyN(f, conn, (fileSize - receivedBytes))
                        if err != nil {
                            // Get the nodeId/IP of the file sender
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
                if success {
                    f.Close()
                    // [TODO] move file to shared only after receiving a completion ACK from master
                    err := os.Rename(temp_dir + sdfsFilename, shared_dir + sdfsFilename)
                    if err != nil {
                        log.Printf("[ME %d] Could not move file %s from tmp to shared dir", myVid, sdfsFilename)
                    }
                    // Send an ACK to Master
                    fmt.Fprintf(conn, "DONE\n")
                }
            case "deletefile":
                sdfsFilename := split_message[1]
                // path := shared_dir + sdfsFilename

                _, err := os.Stat(shared_dir + sdfsFilename)
                if os.IsNotExist(err) {
                    log.Printf("[ME %d] Got a deletefile for %s, but the file does not exist\n", myVid, sdfsFilename)
                    break
                }

                err = os.Remove(shared_dir + sdfsFilename)
                if err != nil {
                    log.Printf("[ME %d] Got a deletefile for %s, the file exists but could not delete it\n", myVid, sdfsFilename)
                    break
                }

                log.Printf("[ME %d] Successfully deleted the file %s\n", myVid, sdfsFilename)   
        }
        conn.Close()
    }
}

func getmyIP() (string) {
    var myip string
    addrs, err := net.InterfaceAddrs()
    if err != nil {
        log.Fatalf("Cannot get my IP")
        os.Exit(1)
    }
    for _, a := range addrs {
        if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
            if ipnet.IP.To4() != nil {
                myip = ipnet.IP.String()
            }
        }
    }
    return myip
}

var fileMap = make(map[string]*fileData)
var nodeMap = make(map[int]*nodeData)

// var filePutTimeMap = make(map[string]int64)

// func putFileMaster()

// [TODO] change nodeMap to contain a map rather than a list
func deleteFromList(nodeId int, sdfsFilename string) {
    idx := -1
    for i, filename := range(nodeMap[nodeId].fileNames) {
        if filename == sdfsFilename {
            idx = i
            break
        }
    }
    if idx != -1 {
        nodeMap[nodeId].fileNames[idx] = nodeMap[nodeId].fileNames[len(nodeMap[nodeId].fileNames)-1]
        nodeMap[nodeId].fileNames = nodeMap[nodeId].fileNames[:len(nodeMap[nodeId].fileNames)-1]
    } else {
        log.Printf("[ME %d] Could not find filename %s in the nodeMap of %d\n", myVid, sdfsFilename, nodeId)
    }
}

func listenMasterRequests() {
    ln, _ := net.Listen("tcp", ":" + strconv.Itoa(masterPort))
    for {
        conn, _ := ln.Accept()
        log.Printf("[Master] Accepted a new connection\n")     

        conn_reader := bufio.NewReader(conn)

        message, _ := conn_reader.ReadString('\n')
        message = message[:len(message)-1]
        fmt.Printf(message)
        split_message := strings.Split(message, " ")
        message_type := split_message[0]

        switch message_type {
        case "put":
            // master should give a list of three other nodes

            sdfsFilename := split_message[1]
            // lastputtime, ok := filePutTimeMap[sdfsFilename]

            _, ok := fileMap[sdfsFilename]
            if ok {

                var nodes_str = ""
                for _, node := range(fileMap[sdfsFilename].nodeIds) {
                    nodes_str = nodes_str + strconv.Itoa(node) + ","
                }
                nodes_str = nodes_str[:len(nodes_str)-1]

                // nodes_str := strings.Join(fileMap[sdfsFilename].nodeIds, ",")
                reply := fmt.Sprintf("putreply %s %s\n", sdfsFilename, nodes_str)
                fmt.Fprintf(conn, reply)

                // [TODO] Add a goroutime to handle ACK from the requester

                // reader := bufio.NewReader(conn)
                ack, err := conn_reader.ReadString('\n')

                fmt.Printf("MASTER ACK: %s\n", ack)

                if err != nil {
                    log.Println("[Master] Error while reading ACK from %d for %s file", myVid, conn.RemoteAddr().String(), sdfsFilename)
                }
                ack = ack[:len(ack)-1]

                if ack == "quorum" {
                    fileMap[sdfsFilename].timestamp = time.Now().Unix()
                    fmt.Printf("Received quorum for %s\n", sdfsFilename)
                    // [TODO] after quorum, send message to nodes; move file from temp to shared
                }  

            } else {
                nodes := getRandomNodes()
                var nodes_str = ""
                for _, node := range(nodes) {
                    nodes_str = nodes_str + strconv.Itoa(node) + ","
                }
                nodes_str = nodes_str[:len(nodes_str)-1]
                fmt.Printf("master nodes str: %s\n", nodes_str)
                // nodes_str := strings.Join(nodes, ",")
                reply := fmt.Sprintf("putreply %s %s\n", sdfsFilename, nodes_str)
                fmt.Printf("%s\n", reply)
                fmt.Fprintf(conn, reply)

                // reader := bufio.NewReader(conn)
                ack, err := conn_reader.ReadString('\n')
                if err != nil {
                    log.Println("[Master] Error while reading ACK from %d for %s file", myVid, conn.RemoteAddr().String(), sdfsFilename)
                }
                ack = ack[:len(ack)-1]

                if ack == "quorum" {
                    var newfiledata fileData
                    newfiledata.timestamp = time.Now().Unix()
                    newfiledata.nodeIds = nodes
                    fileMap[sdfsFilename] = &newfiledata

                    for _, node := range(nodes) {
                        nodedata, ok := nodeMap[node]
                        if ok {
                            nodedata.fileNames = append(nodedata.fileNames, sdfsFilename)
                        } else {
                            var newnodedata nodeData
                            newnodedata.fileNames = []string {sdfsFilename}
                            nodeMap[node] = &newnodedata
                        }
                    }
                    fmt.Printf("Received quorum for %s\n", sdfsFilename)
                    // [TODO] after quorum, send message to nodes; move file from temp to shared
                } 
            }

            // Have to check for cases when one of the nodes fail midway

        case "get":
            sdfsFilename := split_message[1]
            _, ok := fileMap[sdfsFilename]
            var nodes_str = ""

            if ok {
                // send back the list of nodes
                // nodes_str := strings.Join(nodes, ",")
                for _, node := range(fileMap[sdfsFilename].nodeIds) {
                    nodes_str = nodes_str + strconv.Itoa(node) + ","
                }
                nodes_str = nodes_str[:len(nodes_str)-1]                
            }
            reply := fmt.Sprintf("getreply %s %s\n", sdfsFilename, nodes_str)
            fmt.Fprintf(conn, reply)

        case "delete":
            sdfsFilename := split_message[1]
            _, ok := fileMap[sdfsFilename]
            if ok {
                // [TODO] Delete that file from the fileMap
                for _, nodeId := range(fileMap[sdfsFilename].nodeIds) {
                    go deleteFile(nodeId, sdfsFilename)

                } 
                delete(fileMap, sdfsFilename)
            } 

            // do you want to handle else?
        }
        conn.Close()
    }
}

func deleteFile(nodeId int, sdfsFilename string) {
    timeout := time.Duration(20) * time.Second

    ip := memberMap[nodeId].ip
    port := fileTransferPort

    conn, err := net.DialTimeout("tcp", ip + ":" + strconv.Itoa(port), timeout)
    if err != nil {
        log.Printf("[ME %d] Unable to dial a connection to %d (to delete file %s)\n", myVid, nodeId, sdfsFilename)
        return
    }
    defer conn.Close()

    message := fmt.Sprintf("deletefile %s", sdfsFilename)
    padded_message := fillString(message, 64)
    conn.Write([]byte(padded_message))

    // fmt.Fprintf(conn, message)

    log.Printf("[ME %d] Sent deletefile %s to %d\n", myVid, sdfsFilename, nodeId)
}

func getFile(nodeId int, sdfsFilename string, localFilename string) {
    timeout := time.Duration(20) * time.Second

    ip := memberMap[nodeId].ip
    port := fileTransferPort

    conn, err := net.DialTimeout("tcp", ip + ":" + strconv.Itoa(port), timeout) 
    if err != nil {
        log.Printf("[ME %d] Unable to dial a connection to %d (to get file %s)\n", myVid, nodeId, sdfsFilename)
        return
    }
    defer conn.Close()

    message := fmt.Sprintf("getfile %s", sdfsFilename)

    fmt.Fprintf(conn, message)

    bufferFileSize := make([]byte, 10)

    conn.Read(bufferFileSize)
    fileSize, _ := strconv.ParseInt(strings.Trim(string(bufferFileSize), ":"), 10, 64)

    log.Println("[ME %d] Incoming file size %d", myVid, fileSize)

    file, err := os.Create(local_dir + localFilename)
    if err != nil {
        log.Println("[ME %d] Cannot create the local file %s", myVid, localFilename) 
    }
    defer file.Close()

    var receivedBytes int64
    success := true
    for {
        if (fileSize - receivedBytes) < BUFFERSIZE {
            _, err = io.CopyN(file, conn, (fileSize - receivedBytes))
            if err != nil {
                log.Printf("[ME %d] Cannot read from the connection to %d\n", myVid, nodeId)
                success = false
                break
            }
            break
        }
        _, err = io.CopyN(file, conn, BUFFERSIZE)
        if err != nil {
            log.Printf("[ME %d] Cannot read from the connection to %d\n", myVid, nodeId)
            success = false
            break
        }
        receivedBytes += BUFFERSIZE
    }
    if success {
        log.Printf("[ME %d] Successfully received file %s from %d\n", myVid, sdfsFilename, nodeId)
    }
    return
}

func copyFile(srcFile string, destFile string) (bool){
    input, err := ioutil.ReadFile(srcFile)
    if err != nil {
        log.Printf("[ME %d] Cannot read file from %s", srcFile)
        return false
    }

    err = ioutil.WriteFile(destFile, input, 0644)
    if err != nil {
        log.Printf("[ME %d] Cannot write file to %s", destFile)
        return false
    }
    return true
}

func sendFile(nodeId int, localFilename string, sdfsFilename string, wg *sync.WaitGroup) {
    fmt.Printf("Inside send file\n")
    if nodeId == myVid {
        success := copyFile(local_dir + localFilename, shared_dir + sdfsFilename)

        if success {
            wg.Done()
        }
        return
    }

    timeout := time.Duration(20) * time.Second

    ip := memberMap[nodeId].ip
    port := fileTransferPort

    conn, err := net.DialTimeout("tcp", ip + ":" + strconv.Itoa(port), timeout) 
    if err != nil {
        log.Printf("[ME %d] Unable to dial a connection to %d (to send file %s)\n", myVid, nodeId, sdfsFilename)
        return
    }
    defer conn.Close()

    message := fmt.Sprintf("putfile %s", sdfsFilename)
    padded_message := fillString(message, 64)
    fmt.Printf("%s\n", padded_message)

    conn.Write([]byte(padded_message))

    // fmt.Fprintf(conn, message)
    fmt.Printf("Sent a putfile request to %d\n", nodeId)

    f, err := os.Open(local_dir + localFilename)
    if err != nil {
        log.Printf("[ME %d] Cannot open local file %s\n", localFilename)
        return
    }

    fileInfo, err := f.Stat()
    if err != nil {
        log.Printf("[ME %d] Cannot get file stats for %s\n", myVid, localFilename)
        return
    }

    fileSize := fillString(strconv.FormatInt(fileInfo.Size(), 10), 10)
    log.Printf("[ME %d] Sending file %s of size %s to %d", myVid, localFilename, fileSize, nodeId)

    conn.Write([]byte(fileSize))

    sendBuffer := make([]byte, BUFFERSIZE)

    for {
        _, err = f.Read(sendBuffer)
        if err != nil {
            if err == io.EOF {
                break
            } else {
                log.Printf("[ME %d] Error while reading file %s", myVid, localFilename)
                // [TODO] Additional error handling
            }
        }

        _, err = conn.Write(sendBuffer)
        if err != nil{
            log.Printf("[ME %d] Could not send %s file bytes to %d", myVid, localFilename, nodeId)
            // [TODO] Additional error handling
        }

    }
    log.Printf("[ME %s] Successfully sent the file %s to %d\n", myVid, localFilename, nodeId)

    reader := bufio.NewReader(conn)
    ack, err := reader.ReadString('\n')
    fmt.Printf("unproc ack: %s", ack)
    if err != nil {
        log.Println("[ME %d] Error while reading ACK from %d for %s file", myVid, nodeId, sdfsFilename)
    }
    ack = ack[:len(ack)-1]
    fmt.Printf("proc ack: %s", ack)


    if ack == "DONE" {
        fmt.Printf("Received an ack from node %d\n", nodeId)
        wg.Done()
    }  
}

var fileTimeMap = make(map[string]int64)

func executeCommand(command string) {

    timeout := time.Duration(20) * time.Second

    conn, err := net.DialTimeout("tcp", masterIP + ":" + strconv.Itoa(masterPort), timeout)
    if err != nil {
        log.Println("[ME %d] Unable to connect with the master ip=%s port=%d", myVid, masterIP, masterPort)
        return
    }
    defer conn.Close()

    split_command := strings.Split(command, " ")
    command_type := split_command[0]

    switch command_type {
    case "ls":
        sdfsFilename := split_command[1]

        master_command := fmt.Sprintf("get %s\n", sdfsFilename)
        fmt.Fprintf(conn, master_command)

        reader := bufio.NewReader(conn)
        reply, err := reader.ReadString('\n')
        if err != nil {
            log.Printf("[ME %d] Could not read reply from master (for ls %s)\n", myVid, sdfsFilename)
        }
        reply = reply[:len(reply)-1]
        split_reply := strings.Split(reply, " ")

        fmt.Printf("%s: ", split_reply[1])
        nodeIds := strings.Split(split_reply[2], ",")
        for _, nodeId := range nodeIds {
            fmt.Printf("%s ", nodeId)
        } 
        fmt.Printf("\n")

    case "get":
        sdfsFilename := split_command[1]
        localFilename := split_command[2]

        master_command := fmt.Sprintf("get %s\n", sdfsFilename)
        fmt.Fprintf(conn, master_command)

        reader := bufio.NewReader(conn)
        reply, err := reader.ReadString('\n')
        if err != nil {
            log.Printf("[ME %d] Could not read reply from master (for get %s)\n", myVid, sdfsFilename)
        }
        reply = reply[:len(reply)-1]
        split_reply := strings.Split(reply, " ")

        if len(split_reply[2]) == 0 {
            fmt.Printf("invalid file name\n")
            break
        }

        nodeIds_str := strings.Split(split_reply[2], ",")

        fmt.Printf("nodestr: %v %d\n", nodeIds_str, len(nodeIds_str))

        nodeIds := []int{}
        for _, node_str := range nodeIds_str {
            node, err := strconv.Atoi(node_str)
            if err != nil {
                panic(err)
            }
            nodeIds = append(nodeIds, node)
        }

        getFile(nodeIds[0], sdfsFilename, localFilename)

    case "delete":
        fmt.Fprintf(conn, command + "\n")
        log.Printf("[ME %d] Forwarded the %s command to the master\n", myVid, command)

    case "put":
        localFilename := split_command[1] 
        sdfsFilename := split_command[2]

        master_command := fmt.Sprintf("put %s\n", sdfsFilename)
        fmt.Printf("%s\n", master_command)
        fmt.Fprintf(conn, master_command)

        reader := bufio.NewReader(conn)
        reply, err := reader.ReadString('\n')
        if err != nil{
            log.Printf("")
        }
        reply = reply[:len(reply)-1]
        fmt.Printf("%s\n", reply)
        split_reply := strings.Split(reply, " ")
        nodeIds_str := strings.Split(split_reply[2], ",")
        nodeIds := []int{}
        for _, node_str := range nodeIds_str {
            node, err := strconv.Atoi(node_str)
            if err != nil {
                panic(err)
            }
            nodeIds = append(nodeIds, node)
        }

        fmt.Printf("Got nodeIds %v, prepare to send file\n", nodeIds)

        var wg sync.WaitGroup
        wg.Add(4)

        for _, node := range nodeIds {
            go sendFile(node, localFilename, sdfsFilename, &wg)
        }
        fmt.Printf("Waiting for quorum\n")
        wg.Wait()
        fmt.Fprintf(conn, "quorum\n")
        fmt.Printf("sent quorum to master\n")
    }
}

func scanCommands() {
    for {
        reader := bufio.NewReader(os.Stdin)
        command, _ := reader.ReadString('\n')
        command = command[:len(command)-1]
        split_command := strings.Split(command, " ")
        command_type := split_command[0]
        switch command_type {
        case "store":
            for fileName := range(fileTimeMap) {
                fmt.Printf("%s ", fileName)
            }
        case "ls", "get", "delete", "put":
            if command_type == "get" {
                if len(split_command) < 3 {
                    break
                }
            }
            executeCommand(command)
        }
    }
}




    