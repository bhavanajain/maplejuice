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
    "math/rand"
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
type conflictData struct{
    id int
    timestamp int64
}

var local_dir = "local/"
var shared_dir = "shared/"
var temp_dir = "temp/"

var replicatePeriod = 10

var masterIP = "172.22.152.106"
var masterPort = 8084
var fileTransferPort = 8085

// var myVid int

func fillString(returnString string, toLength int) string {
    for {
        lengthString := len(returnString)
        if lengthString < toLength {
            returnString = returnString + ":"
            continue
        }
        break
    }
    return returnString
}

// func getRandomNodes() []int {
//     nodes := []int{1, 2, 3, 4}
//     return nodes
// }

// func findRandomNode() int{ // temporary function
//     node := 5
//     return node
// }

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
        log.Printf("[ME %d] Accepted a new connection from %s\n", myVid)     
        bufferMessage := make([]byte, 64)
        
        conn.Read(bufferMessage)
        message := strings.Trim(string(bufferMessage), ":")
        
        fmt.Printf("[ME %d] Received a new message %s\n", myVid, message)

        split_message := strings.Split(message, " ")
        message_type := split_message[0]

        switch message_type {
            case "getfile":
                sdfsFilename := split_message[1]
                f, err := os.Open(shared_dir + sdfsFilename)
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
                log.Printf("[ME %d] filesize %s", myVid, fileSize)
                conn.Write([]byte(fileSize))

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
                sender := split_message[2]

                fmt.Printf("%s %s\n", message_type, sdfsFilename)

                bufferFileSize := make([]byte, 10)
                conn.Read(bufferFileSize)

                fmt.Printf("%s\n", string(bufferFileSize))
                fileSize, _ := strconv.ParseInt(strings.Trim(string(bufferFileSize), ":"), 10, 64)

                log.Printf("[ME %d] Incoming filesize %d\n", myVid, fileSize)

                tempFilePath = temp_dir + sdfsFilename + "." + sender
                f, err := os.Create(tempFilePath)
                if err != nil {
                    log.Printf("[ME %d] Cannot create file %s\n", myVid, sdfsFilename) 
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

                // Send an ACK to the sender
                fmt.Fprintf(conn, "done\n")

                f.Close()

                if success {
                    log.Printf("Successfully received %s file  from %d\n", sdfsFilename, sender)
                } else {
                    log.Printf("Couldn't successfully receive the file %s from %d\n", sdfsFilename, sender)
                }

            case "ack":
                sdfsFilename := split_message[1]
                sender := split_message[2]

                tempFilePath = temp_dir + sdfsFilename + "." + sender
                sharedFilePath = shared_dir + sdfsFilename

                err := os.Rename(tempFilePath, sharedFilePath)
                if err != nil {
                    log.Printf("[ME %d] Could not move file %s to %s\n", myVid, tempFilePath, sharedFilePath)
                    break
                }

                log.Printf("[ME %d] Successfully moved file from %s to %s\n", myVid, tempFilePath, sharedFilePath)
            
            case "deletefile":
                sdfsFilename := split_message[1]

                filePath := shared_dir + sdfsFilename

                _, err := os.Stat(filePath)
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

            case "replicatefile":
                // Replicate a file in its shared dir to dest node's shared dir
                sdfsFilename := split_message[1]
                destNode, _ := strconv.Atoi(split_message[2])

                go replicateFile(destNode, sdfsFilename) 
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
var conflictMap = make(map[string]*conflictData)

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
        // Need to know the sender id

        switch message_type {
        case "put":
            // master should give a list of three other nodes

            sdfsFilename := split_message[1]
            sender, _ := strconv.Atoi(split_message[2])
            // lastputtime, ok := filePutTimeMap[sdfsFilename]

            // Check in the conflict map for an entry
            _,ok = conflictMap[sdfsFilename]
            if ok {
                // Handle the conflict
                if time.Now().Unix() - conflictMap[sdfsFilename].timestamp < 60 {
                    // There is a conflict, ask the sender
                    reply := fmt.Sprintf("conflict %s\n", sdfsFilename)
                    fmt.Fprintf(conn, reply)
                    // Wait for their reply for a timeout
                    conn.SetReadDeadline(time.Now().Add(30*time.Second))
                    confResp,err := conn_reader.ReadString('\n')
                    if err != nil{
                        // Close the connection and don't procees
                        break
                    }
                    confResp := confResp[:len(confResp)-1]
                    if confResp == "yes"{
                        
                    }



                } else{
                    // Update the conflict map
                    var newConflict conflictData
                    newConflict.id = sender
                    newConflict.timestamp = time.Now().Unix()
                    conflictMap[sdsFileName] = &newConflict
                }

            } else{

                // do nothing
                var newConflict conflictData
                newConflict.id = sender
                newConflict.timestamp = time.Now().Unix()
                conflictMap[sdsFileName] = &newConflict
            }


            _, ok = fileMap[sdfsFilename]
            if ok {

                var nodes_str = ""
                for _, node := range(fileMap[sdfsFilename].nodeIds) {
                    nodes_str = nodes_str + strconv.Itoa(node) + ","
                }
                nodes_str = nodes_str[:len(nodes_str)-1]

                // nodes_str := strings.Join(fileMap[sdfsFilename].nodeIds, ",")
                reply := fmt.Sprintf("putreply %s %s\n", sdfsFilename, nodes_str)
                fmt.Fprintf(conn, reply)

                // Should end the connection here
                // [TODO] Add a goroutime to handle ACK from the requester

                // reader := bufio.NewReader(conn)
                ack, err := conn_reader.ReadString('\n')

                fmt.Printf("MASTER ACK: %s\n", ack)

                if err != nil {
                    log.Printf("[Master] Error while reading ACK from %d for %s file", myVid, conn.RemoteAddr().String(), sdfsFilename)
                }
                ack = ack[:len(ack)-1]

                if ack == "quorum" {
                    fileMap[sdfsFilename].timestamp = time.Now().Unix()
                    fmt.Printf("Received quorum for %s\n", sdfsFilename)
                    // [TODO] after quorum, send message to nodes; move file from temp to shared
                }  

            } else {
                nodes := getRandomNodes(3)
                nodes = append(nodes, sender)

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

                // Should end the connection here

                // reader := bufio.NewReader(conn)
                ack, err := conn_reader.ReadString('\n')
                if err != nil {
                    log.Printf("[Master] Error while reading ACK from %d for %s file", myVid, conn.RemoteAddr().String(), sdfsFilename)
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

        case "ack": 
            action = split_message[1]
            if action == "put" {

            } else if action == "replicate" {

            }
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

    log.Printf("[ME %d] Incoming file size %d", myVid, fileSize)

    file, err := os.Create(local_dir + localFilename)
    if err != nil {
        log.Printf("[ME %d] Cannot create the local file %s", myVid, localFilename) 
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

var ackTimeOut = 5

func replicateFile(nodeId int, sdfsFilename string) {
    timeout := 20 * time.Second

    ip := memberMap[nodeId].ip
    port := fileTransferPort

    conn, err := net.DialTimeout("tcp", ip + ":" + strconv.Itoa(port), timeout) 
    if err != nil {
        log.Printf("[ME %d] Unable to dial a connection to %d (to replicate file %s)\n", myVid, nodeId, sdfsFilename)
        return
    }
    defer conn.Close()

    message := fmt.Sprintf("putfile %s %s", sdfsFilename, myVid)
    padded_message := fillString(message, 64)
    conn.Write([]byte(padded_message))

    fmt.Printf("Sent a putfile %s request to %d\n", sdfsFilename, nodeId)

    f, err := os.Open(shared_dir + sdfsFilename)
    if err != nil {
        log.Printf("[ME %d] Cannot open shared file %s\n", sdfsFilename)
        return
    }

    fileInfo, err := f.Stat()
    if err != nil {
        log.Printf("[ME %d] Cannot get file stats for %s\n", myVid, sdfsFilename)
        return
    }

    fileSize := fillString(strconv.FormatInt(fileInfo.Size(), 10), 10)
    log.Printf("[ME %d] Sending file %s of size %s to %d\n", myVid, sdfsFilename, fileSize, nodeId)
    conn.Write([]byte(fileSize))

    sendBuffer := make([]byte, BUFFERSIZE)

    for {
        _, err = f.Read(sendBuffer)
        if err != nil {
            if err == io.EOF {
                break
            } else {
                log.Printf("[ME %d] Error while reading file %s\n", myVid, sdfsFilename)
                return
            }
        }

        _, err = conn.Write(sendBuffer)
        if err != nil{
            log.Printf("[ME %d] Could not send %s file bytes to %d\n", myVid, sdfsFilename, nodeId)
            return
        }

    }
    log.Printf("[ME %s] Successfully sent the file %s to %d\n", myVid, sdfsFilename, nodeId)

    conn.SetReadDeadline(time.Now().Add(ackTimeOut * time.Second))

    reader := bufio.NewReader(conn)
    ack, err := reader.ReadString('\n')
    if err != nil {
        log.Printf("[ME %d] Error while reading ACK from %d for %s file", myVid, nodeId, sdfsFilename)
        return false
    }
    ack = ack[:len(ack)-1]

    if ack == "done" {
        return true
    } else {
        return false
    }
}

func sendAcktoMaster(action string, srcNode int, destNode int, fileName string) {
    ack_message := fmt.Sprintf("ack %s %d %d %s", action, srcNode, destNode, fileName)

    timeout := time.Duration(20) * time.Second

    conn, err := net.DialTimeout("tcp", masterIP + ":" + strconv.Itoa(masterPort), timeout)
    if err != nil {
        log.Printf("[ME %d] Unable to connect with the master ip=%s port=%d", myVid, masterIP, masterPort)
        return
    }
    defer conn.Close()

    fmt.Fprintf(conn, ack_message)
    return
}

func sendFile(nodeId int, localFilename string, sdfsFilename string, wg *sync.WaitGroup) {

    if nodeId == myVid {
        success := copyFile(local_dir + localFilename, temp_dir + sdfsFilename + "." + strconv.Itoa(nodeId))

        if success {
            wg.Done()
        }
        return
    }

    timeout := 20 * time.Second

    ip := memberMap[nodeId].ip
    port := fileTransferPort

    conn, err := net.DialTimeout("tcp", ip + ":" + strconv.Itoa(port), timeout) 
    if err != nil {
        log.Printf("[ME %d] Unable to dial a connection to %d (to send file %s)\n", myVid, nodeId, sdfsFilename)
        return
    }
    defer conn.Close()

    message := fmt.Sprintf("putfile %s %s", sdfsFilename, myVid)
    padded_message := fillString(message, 64)
    conn.Write([]byte(padded_message))

    fmt.Printf("Sent a putfile %s request to %d\n", sdfsFilename, nodeId)

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
            }
        }

        _, err = conn.Write(sendBuffer)
        if err != nil{
            log.Printf("[ME %d] Could not send %s file bytes to %d", myVid, localFilename, nodeId)
            // request another node to write this file from master
            newnode = replaceNode(nodeId, sdfsFilename)
            return
        }

    }
    log.Printf("[ME %s] Successfully sent the file %s to %d\n", myVid, localFilename, nodeId)

    conn.SetReadDeadline(time.Now().Add(ackTimeOut * time.Second))

    reader := bufio.NewReader(conn)
    ack, err := reader.ReadString('\n')
    if err != nil {
        log.Printf("[ME %d] Error while reading ACK from %d for %s file\n", myVid, nodeId, sdfsFilename)
        newnode = replaceNode(nodeId, sdfsFilename)
        go sendFile(newnode, localFilename, sdfsFilename, &wg)
        return
    }
    ack = ack[:len(ack)-1]

    if ack == "done" {
        fmt.Printf("Received an ack from node %d\n", nodeId)
        wg.Done()
    }  
}

func replaceNode(oldnode int, sdfsFilename string) {
    timeout := 20 * time.Second

    conn, err := net.DialTimeout("tcp", masterIP + ":" + strconv.Itoa(masterPort), timeout)
    if err != nil {
        log.Printf("[ME %d] Unable to connect with the master ip=%s port=%d", myVid, masterIP, masterPort)
        return
    }
    defer conn.Close()

    master_command := fmt.Sprintf("replace %s %s", oldnode, sdfsFilename)
    fmt.Fprintf(conn, master_command)

    reader := bufio.NewReader(conn)
    reply, err := reader.ReadString('\n')
    if err != nil {
        
    }

}

var fileTimeMap = make(map[string]int64)

func executeCommand(command string) {

    timeout := 20 * time.Second

    conn, err := net.DialTimeout("tcp", masterIP + ":" + strconv.Itoa(masterPort), timeout)
    if err != nil {
        log.Printf("[ME %d] Unable to connect with the master ip=%s port=%d", myVid, masterIP, masterPort)
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
        // please send your ID with the message, so put will be "put sdsFileName myVID"
        localFilename := split_command[1] 
        sdfsFilename := split_command[2]

        master_command := fmt.Sprintf("put %s %d\n", sdfsFilename,myVid)
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


// func putFileShared(sourceId int, destId int, fileName string) int{ // the final node to which the data is sent
//     // Master is doing this
//     timeout := time.Duration(20) * time.Second
//     ip := memberMap[nodeId].ip
//     port := fileTransferPort

//     conn, err := net.DialTimeout("tcp", ip + ":" + strconv.Itoa(port), timeout)
//     if err != nil {
//         log.Printf("[ME %d] Unable to connect with the master ip=%s port=%d", myVid, ip, port)
//         return
//     }
//     defer conn.Close()

//     // Send this command to distribute the file to other nodes
//     message := fmt.Sprintf("distributefile %d %s\n",destId,fileName)
//     fmt.Fprintf(conn,message)
//     // Read the message back from the source_ID
//     return destId

   
    

// }

func getRandomNodes(excludeList []int, count int) int {
    success = true

    result := []int{}

    for {
        randomnode = rand.Intn(len(memberMap))
        if !memberMap[randomnode].alive {
            continue
        }
        for _, excludenode := excludeList {
            if randomnode == excludenode {
                success = false
                break
            }
        }
        if success {
            result = result.append(randomnode)
            if len(result) == count {
                return result
            }
        }
    }
}

func initiateReplica(fileName string, srcNode int, destNode int) {
    timeout := time.Duration(20) * time.Second

    ip := memberMap[srcNode].ip
    port := fileTransferPort

    conn, err := net.DialTimeout("tcp", ip + ":" + strconv.Itoa(port), timeout)
    if err != nil {
        log.Printf("[ME %d] Unable to dial a connection to %d (to replicate file %s)\n", myVid, srcNode, fileName)
        return
    }
    defer conn.Close()

    message := fmt.Sprintf("replicate %s %d", fileName, destNode)
    padded_message := fillString(message, 64)
    conn.Write([]byte(padded_message))

    log.Printf("[ME %d] Sent replicate %s file message from %d (at %d)\n", myVid, fileName, srcNode, destNode)
    return
}


func replicateFiles (subjectNode int) {
    // This function tries to make a replica of all files stored by subjectNode.

    for i, fileName := range nodeMap[subjectNode].fileNames {

        // Remove from the fileMap node list
        filenodes := fileMap[fileName].nodeIds
        idx := -1
        for i, node := range filenodes {
            if node == subjectNode {
                idx = i
                break
            }
        }
        filenodes[idx] = filenodes[len(filenodes)-1]
        filenodes = filenodes[:len(nodes)-1]

        newnode = getRandomNodes(filenodes, 1)

        initiateReplica(fileName, filenodes[0], newnode)



                

        // Find a new node to send the file to
        // newNodeId := findRandomNode(nodes) // given the list find something outside it
        // // select the first node or any random node to send the file to this node
        // act_newNodeId = putFileShared(nodes[0], newNodeId, fileName) // What of the nodes[0] fail too, do that later
        // nodes = append(nodes,act_newNodeId)
        // var newfiledata fileData
        // newfiledata.timestamp = fileName[fileMap].timestamp
        // newfiledata.nodeIds = nodes
        // fileMap[fileName] = &newfiledata

    }

}

func ReplicateFile() {
    for{
        if myip == masterIP{
            // Run the code

            for _,fileName := range(fileMap){
                if len(fileMap[fileMap].nodeIds) < 4{
                    // Copy files
                }
            }

        }

        time.Sleep(time.Duration(replicatePeriod) * time.Second)
    }
}




    