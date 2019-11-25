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
    "os/exec"
    "io/ioutil"
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

func sendKeyFile(action string, srcNode int, taskId int, keysFilename string) {
    message := fmt.Sprintf("keyfile %s %d %d", action, srcNode, taskId)
    padded_message := fillString(message, messageLength)

    timeout := time.Duration(20) * time.Second
    conn, err := net.DialTimeout("tcp", masterIP + ":" + strconv.Itoa(fileTransferPort), timeout)
    if err != nil {
        log.Printf("[ME %d] Unable to connect with the master ip=%s port=%d", myVid, masterIP, masterPort)
        return
    }

    conn.Write([]byte(padded_message))
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

func ExecuteCommand(exeFile string, inputFilePath string, outputFilePath string, mapleId int) {

    err := os.Chmod(exeFile, 0777)
    if err != nil {
        fmt.Printf("%v\n", err)
    }

    run_cmd := fmt.Sprintf("./%s -inputfile %s", exeFile, inputFilePath)
    fmt.Printf("Trying to run %s\n", run_cmd)

    cmd := exec.Command("sh","-c", run_cmd)
    outfile, err := os.Create(outputFilePath)
    if err != nil {
        fmt.Printf("%v\n", err)
        panic(err)
    }

    stdoutPipe, err := cmd.StdoutPipe()
    if err != nil {
        panic(err)
    }

    writer := bufio.NewWriter(outfile)

    err = cmd.Start()
    if err != nil {
        fmt.Printf("%v\n", err)
        panic(err)
    }

    go io.Copy(writer, stdoutPipe)
    cmd.Wait()

    writer.Flush()
    outfile.Close()

    fmt.Printf("Maple processing done\n")

    // run_cmd = fmt.Sprintf("cat %s", outputFilePath)
    // out, err := exec.Command("sh","-c", run_cmd).Output()
    // fmt.Println(out)

    f, err := os.Open(outputFilePath)
    if err != nil {
        fmt.Printf("Cannot open %s for reading\n", outputFilePath)
    }
    fmt.Printf("opened generated output file for key separation\n")
    // defer f.Close()

    keyFileHandleMap := make(map[string]*os.File)
    // for large output, process in block of 1024 lines

    fileReader := bufio.NewReader(f)
    fileEnd := false
    for {
        line, err := fileReader.ReadString('\n')
        if err != nil {
            if err != io.EOF {
                fmt.Printf("Unknown error encountered while reading file\n")
                break
            }
            fileEnd = true
        }
        if len(line) > 0 {
            fmt.Printf("curr line: %s", line)
            splitLine := strings.Split(line, " ")
            key := splitLine[0]
            _, ok := keyFileHandleMap[key]
            if ok {
                temp := keyFileHandleMap[key]
                temp.WriteString(line)
            } else {
                tempFilePath := fmt.Sprintf(maple_dir + "output_%d_%s.out", mapleId, key)
                tempFile, err := os.Create(tempFilePath)
                if err != nil {
                    fmt.Printf("Could not create the file %s\n", tempFilePath)
                    panic(err)
                } 
                fmt.Printf("created new file handler for %s\n", tempFilePath)
                keyFileHandleMap[key] = tempFile
                tempFile.WriteString(line)
            }
        }
        if fileEnd {
            for _, fhandle := range(keyFileHandleMap) {
                fhandle.Close()
            }
            f.Close()
            break
        }
    }
    
    keysFilename := fmt.Sprintf("keys_%d.info", mapleId)
    keysFileHandle, err := os.Create(keysFilename)
    if err != nil {
        fmt.Printf("Could not create %s\n", keysFilename)
        panic(err)
    }

    for key := range(keyFileHandleMap) {
        keysFileHandle.WriteString(key + "$$$$")
    }
    keysFileHandle.Close()

    sendKeyFile("maple", myVid, mapleId, keysFilename)
}

func AssembleKeyFiles() {
    mapleIdKeysMap := make(map[int][]string)
    keyMapleIdMap := make(map[string][]int)
    for mapleId := range mapleMap {
        keysFilename := fmt.Sprintf("keys_%d.info", mapleId)
        contentBytes, err := ioutil.ReadFile(maple_dir + keysFilename)
        if err != nil {
            fmt.Printf("Could not read file corresponding to %d maple id\n", mapleId)
            panic(err)
        }
        content := string(contentBytes)
        keys := strings.Split(content, "$$$$")
        keys = keys[:len(keys)-1]
        for _, key := range keys {
            _, ok := keyMapleIdMap[key]
            if ok {
                keyMapleIdMap[key] = append(keyMapleIdMap[key], mapleId)
            } else {
                keyMapleIdMap[key] = []int{mapleId}
            }
        }
        fmt.Printf("Keys for %d maple id: %v, len of keys = %d\n", mapleId, keys, len(keys))
        mapleIdKeysMap[mapleId] = keys
    }
    fmt.Printf("%v\n", mapleIdKeysMap)
    fmt.Printf("%v\n", keyMapleIdMap)

    // assign key processing to one node
    allNodes := []int{}
    for nodeId := range memberMap {
        if nodeId == 0 {
            continue
        }
        allNodes = append(allNodes, nodeId)
    }

    keyNodeIdMap := make(map[string]int)

    nodeIdx := 0
    for key := range keyMapleIdMap {
        keyNodeIdMap[key] = allNodes[nodeIdx]
        go ProcessKey(key, allNodes[nodeIdx], keyMapleIdMap[key])
        nodeIdx = (nodeIdx + 1) % len(allNodes)
    }
}

func ProcessKey(key string, nodeId int, mapleIds []int) {
    keysFilename := fmt.Sprintf("%s_node.info", key)
    keyfile, err := os.Create(keysFilename)
    if err != nil {
        fmt.Printf("Could not create %s file\n", keysFilename)
        panic(err)
    }
    for mapleId := range mapleIds {
        // each record is mapleId:nodeId
        line := fmt.Sprintf("%d:%d", mapleId, mapleMap[mapleId])
        keyfile.WriteString(line + "$$$$")
    } 
    keyfile.Close()

    timeout := time.Duration(20) * time.Second

    nodeIP := memberMap[nodeId].ip
    conn, err := net.DialTimeout("tcp", nodeIP + ":" + strconv.Itoa(fileTransferPort), timeout)
    if err != nil {
        log.Printf("[ME %d] Unable to connect with the master ip=%s port=%d", myVid, masterIP, masterPort)
        return
    }

    message := fmt.Sprintf("keyaggr %s", key)
    padded_message := fillString(message, messageLength)

    conn.Write([]byte(padded_message))
    simpleSendFile(conn, keysFilename)
}


func KeyAggregation(key string, nodeInfoList []string) {
    var wg sync.WaitGroup
    wg.Add(len(nodeInfoList))

    for _, nodeInfo := range(nodeInfoList) {
         
        splitNodeInfo:= strings.Split(nodeInfo, ":")
        mapleId_str := splitNodeInfo[0]
        nodeId_str := splitNodeInfo[1]
        nodeId, _ := strconv.Atoi(nodeId_str)
        dataFilename := fmt.Sprintf("output_%s_%s.out", mapleId_str, nodeId_str)
        go getDirFile(nodeId, maple_dir + dataFilename, maple_dir + dataFilename, &wg)
    }
    wg.Wait()

    fmt.Printf("Got all files for key %s aggregation\n", key)
    // APPEND ALL RECVD files into one
    // when everybody is done, aggregate into one file

}


func getDirFile(destNodeId int, destFilePath string, localFilePath string, wg *sync.WaitGroup) (bool) {
    // get file 
    timeout := time.Duration(20) * time.Second

    ip := memberMap[destNodeId].ip
    port := fileTransferPort

    conn, err := net.DialTimeout("tcp", ip + ":" + strconv.Itoa(port), timeout) 
    if err != nil {
        log.Printf("[ME %d] Unable to dial a connection to %d (to get file %s)\n", myVid, destNodeId, destFilePath)
        return false
    }
    defer conn.Close()

    message := fmt.Sprintf("getfile2 %s", destFilePath)
    padded_message := fillString(message, messageLength)
    conn.Write([]byte(padded_message))

    // fmt.Fprintf(conn, message)

    bufferFileSize := make([]byte, 10)

    _, err = conn.Read(bufferFileSize)
    if err != nil {
        fmt.Println(err) // what error are you getting?
        log.Printf("[ME %d] Error while fetching file %s from %d\n", myVid, destFilePath, destNodeId)
        fmt.Printf("[ME %d] Error while fetching file %s from %d\n", myVid, destFilePath, destNodeId)
        return false
    }
    fileSize, _ := strconv.ParseInt(strings.Trim(string(bufferFileSize), ":"), 10, 64)

    log.Printf("[ME %d] Incoming file size %d", myVid, fileSize)
    // fmt.Printf("[ME %d] Incoming file size %d", myVid, fileSize)

    file, err := os.Create(localFilePath)
    if err != nil {
        log.Printf("[ME %d] Cannot create the local file %s", myVid, localFilePath) 
        return false
    }
    defer file.Close()

    var receivedBytes int64
    success := false
    for {
        if (fileSize - receivedBytes) < BUFFERSIZE {
            bytesW, err := io.CopyN(file, conn, (fileSize - receivedBytes))
            receivedBytes += bytesW
            if err != nil {
                log.Printf("[ME %d] Cannot read from the connection to %d\n", myVid, destNodeId)
                break
            }
            // fmt.Printf("%s %s -> filesize = %d,  total bytes received = %d\n", sdfsFilename, localFilename, fileSize, receivedBytes)
            success = true
            break
        }
        _, err = io.CopyN(file, conn, BUFFERSIZE)
        if err != nil {
            log.Printf("[ME %d] Cannot read from the connection to %d\n", myVid, destNodeId)
            break
        }
        receivedBytes += BUFFERSIZE
    }

    if success {
        log.Printf("[ME %d] Successfully received file %s from %d\n", myVid, destFilePath, destNodeId)
        fmt.Printf("[ME %d] Successfully received file %s from %d\n", myVid, destFilePath, destNodeId)
        wg.Done()
        // fmt.Printf("[ME %d] Successfully received file %s from %d\n", myVid, sdfsFilename, nodeId)
    }
    return success
    
}



