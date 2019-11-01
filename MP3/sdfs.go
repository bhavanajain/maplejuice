package main

import (
    "os"
    "net"
    "strings"
    "strconv"
    "net"
    "log"
    "sync"
    "bufio"
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

var masterIP = "172.22.152.106"
var masterPort = 8080
var fileTransferPort = 8081

var myVid int

func listenFileTransferPort() {
    // This port serves file transfers
    // message type: getfile, putfile, deletefile

    ln, _ := net.Listen("tcp", ":" + strconv.Itoa(fileTransferPort))
    for {
        conn, _ := ln.Accept()
        log.Println("[ME %d] Accepted a new connection", myVid)     

        conn_reader := bufio.NewReader(conn)

        message, _ := conn_reader.ReadString('\n')
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

                success = true
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
                    
                    bytesW, err = conn.Write(sendBuffer)
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
                sdfsFilename = split_message[1]

                bufferFileSize := make([]byte, 10)
                conn.Read(bufferFileSize)

                fileSize, _ := strconv.ParseInt(strings.Trim(string(bufferFileSize), ":"), 10, 64)
                log.Println("[ME %d] Incoming filesize %d\n", myVid, fileSize)

                f, err := os.Create(temp_dir + sdfsFilename)
                if err != nil {
                    log.Println("[ME %d] Cannot create file %s\n", myVid, sdfsFilename) 
                }

                var receivedBytes int64
                success = true
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
                    err := os.Rename(temp_dir + sdfsFilename, shared_dir + sdfsFilename)
                    if err != nil {
                        log.Printf("[ME %d] Could not move file %s from tmp to shared dir", myVid, sdfsFilename)
                    }
                    // Send an ACK to Master
                    fmt.Printf(conn, "DONE\n")
                }
            case "deletefile":
                sdfsFilename = split_message[1]
                path := shared_dir + sdfsFilename

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



func listenMasterRequests() {
    ln, _ = net.Listen("tcp", ":" + strconv.Itoa(masterPort))
    for {
        conn, _ := ln.Accept()
        log.Printf("[Master] Accepted a new connection\n")     

        conn_reader := bufio.NewReader(conn)

        message, _ := conn_reader.ReadString('\n')
        split_message := strings.Split(message, " ")
        message_type = split_message[0]

        switch message_type {
        case "put":
            // master should give a list of three other nodes

            sdfsFilename = split_message[1]
            // lastputtime, ok := filePutTimeMap[sdfsFilename]

            nodes, ok := fileMap[sdfsFilename]
            if ok {
                nodes_str := strings.Join(nodes, ",")
                reply := fmt.Sprintf("putreply %s %s\n", sdfsFilename, nodes_str)
                fmt.Printf(conn, reply)
            } else {
                nodes = getRandomNodes()
                nodes_str := strings.Join(nodes, ",")
                reply := fmt.Sprintf("putreply %s %s\n", sdfsFilename, nodes_str)
                fmt.Printf(conn, reply)
            }

            // Have to check for cases when one of the nodes fail midway

        case "get":
            sdfsFilename = split_message[1]
            nodes, ok := fileMap[sdfsFilename]
            if ok {
                // send back the list of nodes
                nodes_str := strings.Join(nodes, ",")
                reply := fmt.Sprintf("getreply %s %s\n", sdfsFilename, nodes_str)
                fmt.Printf(conn, reply)
            } else {
                reply := fmt.Sprintf("invalid %s\n", sdfsFilename)
                fmt.Printf(conn, reply)
            }

        case "delete":
            sdfsFilename = split_message[1]
            nodes, ok := fileMap[sdfsFilename]
            if ok {
                // [TODO] Delete that file from the fileMap
                for _, nodeId := range(nodes) {
                    go deleteFile(nodeId, sdfsFilename)
                }
            } 
            // do you want to handle else?
        }
        conn.Close()
    }
}

func deleteFile(nodeId int, sdfsFilename string) {
    timeout := time.Duration(20) * time.Second

    ip = members[nodeId].ip
    port = fileTransferPort

    conn, err := netDialTimeout("tcp", ip + ":" + port, timeout)
    if err != nil {
        log.Printf("[ME %d] Unable to dial a connection to %d (to delete file %s)\n", myVid, nodeId, sdfsFilename)
        return
    }
    defer conn.Close()

    message = fmt.Sprintf("deletefile %s", sdfsFilename)
    fmt.Printf(conn, message)

    log.Printf("[ME %d] Sent deletefile %s to %d\n", myVid, sdfsFilename, nodeId)
}

func getFile(nodeId int, sdfsFilename string, localFilename string) {
    timeout := time.Duration(20) * time.Second

    ip = members[nodeId].ip
    port = fileTransferPort

    conn, err := net.DialTimeout("tcp", ip + ":" + port, timeout) 
    if err != nil {
        log.Printf("[ME %d] Unable to dial a connection to %d (to get file %s)\n", myVid, nodeId, sdfsFilename)
        return
    }
    defer conn.Close()

    message = fmt.Sprintf("getfile %s", sdfsFilename)

    fmt.Printf(conn, message)

    bufferFileSize := make([]byte, 10)

    conn.Read(bufferFileSize)
    fileSize, _ := strconv.ParseInt(strings.Trim(string(bufferFileSize), ":"), 10, 64)

    log.Println("[ME %d] Incoming file size %d", myVid, fileSize)

    file, err := os.Create(localFilename)
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

    if nodeId == myVid {
        success = copyFile(local_dir + localFilename, shared_dir + sdfsFilename)
        if success {
            wg.Done()
        }
        return
    }

    timeout := time.Duration(20) * time.Second

    ip = members[nodeId].ip
    port = fileTransferPort

    conn, err := net.DialTimeout("tcp", ip + ":" + port, timeout) 
    if err != nil {
        log.Printf("[ME %d] Unable to dial a connection to %d (to send file %s)\n", myVid, nodeId, sdfsFilename)
        return
    }
    defer conn.Close()

    message = fmt.Sprintf("putfile %s", sdfsFilename)

    fmt.Printf(conn, message)

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
    ack, err := reader.ReadString("\n")
    if err != nil {
        log.Println("[ME %d] Error while reading ACK from %d for %s file", myVid, nodeId, sdfsFilename)
    }

    if ack == "DONE" {
        wg.Done()
    }  
}

var fileTimeMap = make(map[string]int64)

func executeCommand(command string) {

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
        sdfsFilename = split_command[1]

        master_command = fmt.Sprintf("get %s\n", sdfsFilename)
        fmt.Fprintf(conn, master_command)

        reader := bufio.NewReader(conn)
        reply, err := reader.ReadString("\n")
        if err != nil {
            log.Printf("[ME %d] Could not read reply from master (for ls %s)\n", myVid, sdfsFilename)
        }
        split_reply = strings.Split(reply, " ")

        log.Printf("%s: ", split_reply[1])
        nodeIds = strings.Split(split_reply[2], ",")
        for _, nodeId := range nodeIds {
            log.Printf("%s ", nodeId)
        } 
        log.Printf("\n")

    case "get":
        sdfsFilename = split_command[1]
        localFilename = split_command[2]

        master_command = fmt.Sprintf("get %s\n", sdfsFilename)
        fmt.Fprintf(conn, master_command)

        reader := bufio.NewReader(conn)
        reply, err := reader.ReadString("\n")
        if err != nil {
            log.Printf("[ME %d] Could not read reply from master (for get %s)\n", myVid, sdfsFilename)
        }
        split_reply := strings.Split(reply, " ")
        nodeIds_str := strings.Split(split_reply[2], ",")

        nodeIds = []int{}
        for _, node_str := range nodeIds_str {
            node, err := strconv.Atoi(node_str)
            if err != nil {
                panic(err)
            }
            nodeIds = append(nodeIds, node)
        }

        getFile(nodeIds[0], sdfsFilename, localFilename)

    case "delete":
        fmt.Printf(conn, command)

    case "put":
        localFilename := split_command[1] 
        sdfsFilename := split_command[2]

        master_command = fmt.Sprintf("put %s", sdfsFilename)
        fmt.Fprintf(conn, master_command)

        reader := bufio.NewReader(conn)
        reply, err := reader.ReadString("\n")
        if err != nil{
            log.Printf("")
        }
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

        var wg sync.WaitGroup
        wg.Add(4)

        for _, node := range nodeIds {
            go sendFile(node, sdfsFilename, &wg)
        }
        wg.Wait()
    }

}


func scanCommands() {
    for {
        reader := bufio.NewReader(os.Stdin)
        command, _ := reader.ReadString('\n')
        split_command := strings.Split(command, " ")
        command_type := split_command[0]
        switch command_type {
        case "store":
            for fileName := range(fileTimeMap) {
                fmt.Printf("%s ", fileName)
            }
        case "ls", "get", "delete", "put":
            executeCommand(command)
        }
    }
}


func main() {
    os.RemoveAll(shared_dir)
    os.MkdirAll(shared_dir, 0777)

    myIP := getmyIP()
    if myIP == masterIP {
        go listenMasterRequests()
    } else {
        go listenFileTransferPort()
        go scanCommands()
    }

}

    