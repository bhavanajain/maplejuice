package main

import (
    "os"
    "net"
    "strings"
    "strconv"
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

local_dir = 'local/'
shared_dir = 'shared/'

masterIP = "172.22.152.106"
masterPort = 8080
fileTransferPort = 8081

var myVid int

func listenFileTransferPort() {
    // This port serves file transfers
    // message type: getfile, putfile, deletefile

    ln, _ = net.Listen("tcp", ":" + strconv.Itoa(fileTransferPort))
    for {
        conn, _ := ln.Accept()
        log.Println("[ME %d] Accepted a new connection", myVid)     

        conn_reader := bufio.NewReader(conn)

        message, _ := conn_reader.ReadString('\n')
        split_message := strings.Split(message, " ")
        message_type = split_message[0]

        switch message_type {

            case "getfile":
                sdfsFilename = split_message[1]
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
        fmt.Println("[Info] Accepted a new connection")     

        conn_reader := bufio.NewReader(conn)

        message, _ := conn_reader.ReadString('\n')
        split_message := strings.Split(message, " ")
        message_type = split_message[0]
        switch message_type {
        case "put":
            // master should give a list of three other nodes
            sdfsFilename = split_message[2]
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
                // ask nodes to delete file
            } else {
                // dunno if we want to send a reply
            }

        }
        conn.Close()
    }
}

func getFile(nodeId int, sdfsFilename string, localFilename string) {
    timeout := time.Duration(20) * time.Second

    ip = members[nodeId].ip
    port = fileTransferPort

    conn, err := net.DialTimeout("tcp", ip + ":" + port, timeout) 
    if err != nil {
        log.Printf("[File Sender] Unable to connect with %s %s\n", nodeid, filename)
        return
    }
    defer conn.Close()

    message = fmt.Sprintf("getfile %s", sdfsFilename)

    fmt.Printf(conn, message)

    // bufferFileName := make([]byte, 64)
    bufferFileSize := make([]byte, 10)

    conn.Read(bufferFileSize)
    fileSize, _ := strconv.ParseInt(strings.Trim(string(bufferFileSize), ":"), 10, 64)

    // conn.Read(bufferFileName)
    // fileName := strings.Trim(string(bufferFileName), ":")

    log.Println("[File Server] Read filesize" ,fileSize)

    file, err := os.Create(localFilename)
    if err != nil {
        log.Println("[File Server Error] Couldn't create the file") 
    }
    defer file.Close()

    var receivedBytes int64
    for {
        if (fileSize - receivedBytes) < BUFFERSIZE {
            _,err = io.CopyN(file, conn, (fileSize - receivedBytes)) // Need to handle the err in the code in case of network failure
            if err != nil{
                // <ToDo>Connection error exit and find the next node 
            }
            break
        }
        _,err = io.CopyN(file, conn, BUFFERSIZE) // Need to handle the err in the code in case of network failure
        if err != nil{
            //<ToDo> Connection error handle it
        }
        receivedBytes += BUFFERSIZE
    }
    return
}

func sendFile(nodeId int, sdfsFilename string) {

    timeout := time.Duration(20) * time.Second

    ip = members[nodeId].ip
    port = fileTransferPort

    conn, err := net.DialTimeout("tcp", ip + ":" + port, timeout) 
    if err != nil {
        log.Printf("[File Sender] Unable to connect with %s %s\n", nodeId, sdfsFilename)
        return
    }
    defer conn.Close()

    message = fmt.Sprintf("putfile %s", sdfsFilename)

    fmt.Printf(conn, message)



    f, err := os.Open(sdfsFilename)
    if err != nil {
        log.Printf("[File Sender]: Can't open file\n")
        return
    }

    fileInfo, err := f.Stat()
    if err != nil {
        log.Printf("[File Sender] Can't access file stats %s %s\n", nodeId, sdfsFilename)
        return
    }

    fileSize := fillString(strconv.FormatInt(fileInfo.Size(), 10), 10)
    fileName := fillString(fileInfo.Name(), 64)

    log.Printf("[File Sender] filesize %s filename %s", fileSize, fileName)

    conn.Write([]byte(fileSize))
    // conn.Write([]byte(fileName)) // Not sending the file name 

    sendBuffer := make([]byte, BUFFERSIZE)

    for {
        _, err = f.Read(sendBuffer)
        if err == io.EOF {
            break
        } else if err != nil{
            //<ToDo> Handle the error
        }
        _,err = conn.Write(sendBuffer) // Handle the error
        if err != nil{
            //<ToDo> Handle the error 
        }

    }
    log.Printf("[File Sender] Completed sending the file %s %s\n", nodeId, sdfsFilename)

    reader := bufio.NewReader(conn)
    ack, err := reader.ReadString("\n")
    if err != nil {
        fmt.Println("Did not receive an ACK")
    }
    if ack == "DONE" {
        // TODO: 
    }


    
}



var fileTimeMap = make(map[string]int64)



func executeCommand(command string) {

    conn, err := net.DialTimeout("tcp", masterIP + ":" + strconv.Itoa(masterPort), timeout)
    if err != nil {
        fmt.Println("[Error] Unable to connect with the master", masterIP)
        return
    }
    defer conn.Close()

    split_command := strings.Split(command, " ")
    command_type := split_command[0]
    switch command_type {
    case "ls":
        sdfsFilename = split_command[1]
        master_command = fmt.Sprintf("get %s\n", sdfsFilename)
        fmt.Fprintf(conn, command)
        reader := bufio.NewReader(conn)
        reply, err := reader.ReadString("\n")
        if err != nil {
            fmt.Printf("Could not read response\n")
        }
        split_reply = strings.Split(reply, " ")
        fmt.Printf("%s: %s\n", split_reply[1], split_reply[2]) // Printing the list of nodes where the file resides

    case "get":
        sdfsFilename = split_command[1]
        localFilename = split_command[2]
        master_command = fmt.Sprintf("get %s\n", sdfsFilename)
        fmt.Fprintf(conn, command) // Sending the file name to master
        reader := bufio.NewReader(conn)
        reply, err := reader.ReadString("\n") // reading from the master
        if err != nil {
            fmt.Printf("Could not read response\n")
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

        // getfile message to nodeIds[0]
        getFile(nodeIds[0], sdfsFilename, localFilename) // Received file
        // <ToDo> need to handle the different nodes in case of failure






    case "delete":
    case "put":
        localFilename := split_command[1] 
        sdfsFilename := split_command[2]
        fmt.Fprintf(conn, command)
        reader := bufio.NewReader(conn)
        reply,err := reader.ReadString("\n")
        if err!= nil{
            fmt.Printf("Couldn't parse input")
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

        for _, node := range nodeIds{
            // Send a wait command so that it will wait til this is finished
            go sendFile(node,sdfsFilename)
        }

        




    }


    fmt.Fprintf(conn, command)
    reader := bufio.NewReader(conn)

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


        // case "ls":
        //  sdfsFilename = split_command[1]
        //  command = fmt.Sprintf("get %s", sdfsFilename)
        //  executeCommand(command)
        // case "get", "delete", "put":
        // //   sdfsFilename = split_command[1]
        // //   command = fmt.Sprintf("%s %s", command_type, sdfsFilename)
        //  executeCommand(command)
        // // case "put":
        // //   sdfsFilename = split_command[2]
        // //   command = fmt.Sprintf()
        // //   executeCommand(command)

        }
    }
}


func main() {
    os.RemoveAll(shared_dir)
    os.MkdirAll(shared_dir, FileMode)

    myIP := getmyIP()
    if myIP == masterIP {
        go listenMasterRequests())
    } else {
        go scanCommands()

    }

}

    