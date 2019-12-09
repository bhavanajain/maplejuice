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
)




func sendMapleInfo(nodeId int, mapleId int, sdfsMapleExe string, inputFile string) {
	timeout := 20 * time.Second

    ip := memberMap[nodeId].ip
    port := fileTransferPort

    acquireConn()
    conn, err := net.DialTimeout("tcp", ip + ":" + strconv.Itoa(port), timeout) 
    if err != nil {
        log.Printf("[ME %d] Unable to dial a connection to %d (to send maple task for %s)\n", myVid, nodeId, sdfsMapleExe)
        releaseConn()

        return
    }
    defer releaseConn()
    defer conn.Close()


    message := fmt.Sprintf("runmaple %d %s %s", mapleId, sdfsMapleExe, inputFile)
    // fmt.Printf("%s\n", message)
    padded_message := fillString(message, messageLength)
    conn.Write([]byte(padded_message))
    // releaseConn()
    return
}

func sendJuiceInfo(nodeId int, mapleId int, sdfsMapleExe string, inputFile string) {
    timeout := 20 * time.Second

    ip := memberMap[nodeId].ip
    port := fileTransferPort

    acquireConn()
    conn, err := net.DialTimeout("tcp", ip + ":" + strconv.Itoa(port), timeout) 
    if err != nil {
        log.Printf("[ME %d] Unable to dial a connection to %d (to send maple task for %s)\n", myVid, nodeId, sdfsMapleExe)
        releaseConn()

        return
    }
    defer releaseConn()
    defer conn.Close()


    message := fmt.Sprintf("runjuice %d %s %s", mapleId, sdfsMapleExe, inputFile)
    // fmt.Printf("%s\n", message)
    padded_message := fillString(message, messageLength)
    conn.Write([]byte(padded_message))
    // releaseConn()
    return
}


func getFileWrapper(sdfsFilename string, localFilename string) bool {
    fmt.Printf("Inside get file wrapper: asking for sdfs: %s local: %s \n", sdfsFilename, localFilename)
    log.Printf("Inside get file wrapper: asking for sdfs: %s local: %s \n", sdfsFilename, localFilename)
    initTime := time.Now()
    // sdfsFilename := split_command[1]
    // localFilename := split_command[2]
    // Create a connection to main to ask for the file
	timeout := 20 * time.Second
    acquireConn()
    conn, err := net.DialTimeout("tcp", masterIP + ":" + strconv.Itoa(getPort), timeout)
    if err != nil {
        log.Printf("[ME %d] Unable to connect with the master ip=%s port=%d", myVid, masterIP, getPort)
        // conn.Close()
        releaseConn()
        return false
    }


    master_command := fmt.Sprintf("get %s\n", sdfsFilename)
    fmt.Fprintf(conn, master_command) // get the running 

    fmt.Printf("sent a get request %s to the master\n", master_command) 
    log.Printf("sent a get request %s to the master\n", master_command) 


    reader := bufio.NewReader(conn)
    reply, err := reader.ReadString('\n')
    if err != nil{
        log.Printf("[ME %d] Could not read reply from master (for get %s)\n", myVid, sdfsFilename)
        fmt.Printf("[ME %d] Could not read reply from master (for get %s) Reply : %s\n", myVid, sdfsFilename,reply)

        conn.Close()
        releaseConn()
        return false
    }

    conn.Close()    // [NEW]
    releaseConn()
    fmt.Printf("get Request Reply from master : %s \n",reply)
    log.Printf("get Request Reply from master : %s \n",reply)
    if len(reply) == 0{
        fmt.Printf("Empty reply received for file %s\n",sdfsFilename)
        return false
    }
    reply = reply[:len(reply)-1]
    split_reply := strings.Split(reply, " ")

    if len(split_reply[2]) == 0 {
        log.Printf("invalid file name\n")
        fmt.Printf("invalid file name\n")
        // conn.Close()
        return false
    }

    nodeIds_str := strings.Split(split_reply[2], ",")

    fmt.Printf("[Get File] nodestr: %v %d\n", nodeIds_str, len(nodeIds_str))

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
        fmt.Printf("[ME %d] Trying to read file %s from %d \n",myVid,sdfsFilename,node)
        log.Printf("[ME %d] Trying to read file %s from %d \n",myVid,sdfsFilename,node)
        success = getFile(node, sdfsFilename, localFilename) // Modify here
        if success {
            fmt.Printf("File received!!! %s %s\n",sdfsFilename,localFilename)
            break
        }
    }

    elapsed := time.Since(initTime)

    if !success {
        fmt.Printf("[ME %d] Could not fetch %s shared to %s local\n", myVid, sdfsFilename, localFilename)
        // return false
    }

    fmt.Printf("Time taken for get %s\n", elapsed)
    return success

}

func PutFileWrapper2(localFilename string, sdfsFilename string) (bool) {
    fmt.Printf("Inside put file wrapper for file %s\n", localFilename)

    _, err := os.Stat(local_dir + localFilename)
    if os.IsNotExist(err) {
        fmt.Printf("Got a put for %s, but the file does not exist\n", localFilename)
        log.Printf("[ME %d] Got a put for %s, but the file does not exist\n", myVid, localFilename)
        return false
    }

    timeout := 20 * time.Second
    acquireConn()
    conn, err := net.DialTimeout("tcp", masterIP + ":" + strconv.Itoa(putPort), timeout)
    if err != nil {
        log.Printf("[ME %d] Unable to connect with the master PUTPORT ip=%s port=%d", myVid, masterIP, putPort)
        releaseConn()
        return false
    }

    master_command := fmt.Sprintf("put %s %d\n", sdfsFilename, myVid)
    fmt.Fprintf(conn, master_command)

    fmt.Printf("Sent the put request to master\n")

    // Add a time out for the connection else move ahead
    conn.SetReadDeadline(time.Now().Add(20*time.Second))
    reader := bufio.NewReader(conn) // Master response
    reply, err := reader.ReadString('\n')
    if err != nil{
        log.Printf(" Can't move forward with put reqest")
        conn.Close()
        releaseConn()
        return false // free up the user request
    }
    reply = reply[:len(reply)-1]
    fmt.Printf("Master reply for put from PUTPORT: %s\n", reply)
    split_reply := strings.Split(reply, " ")
    
    conn.Close()
    releaseConn()

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

    LocdoneList:= []int{}
    myChan := make(chan int , 4)

    fmt.Printf("Sending file to %v\n", nodeIds)

    for _, node := range nodeIds {
        // connguard <- struct{}{}
        go sendFile(node, localFilename, sdfsFilename, &wg, nodeIds,myChan,2)
    }

    wg.Wait()
    for i:= 0;i<4;i++{
        newVal := <-myChan
        if newVal < 0 {
            continue
        } else {
            LocdoneList = append(LocdoneList,newVal)
        }
    }

    doneList_str := list2String(LocdoneList)
    if len(LocdoneList) > 0{
        fmt.Printf("Send ack to master PutFileWrapper2\n")
        sendAcktoMaster("put", myVid, doneList_str, sdfsFilename)
        return true
    } else {
        fmt.Printf("Can't Finish Put request for %d\n",sdfsFilename)
        return false
    }
}



func PutFileWrapper(localFilename string, sdfsFilename string, conn net.Conn) {
    fmt.Printf("Inside put file wrapper for file %s\n", localFilename)

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
    conn.SetReadDeadline(time.Now().Add(10*time.Second))
    reply, err := reader.ReadString('\n')
    if err != nil{
        log.Printf(" Can't move forward with put reqest")
        // conn.Close()
        // releaseConn()
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

    LocdoneList:= []int{}
    myChan := make(chan int , 4)

    fmt.Printf("Sending file to %v\n", nodeIds)

    for _, node := range nodeIds {
        // connguard <- struct{}{}
        go sendFile(node, localFilename, sdfsFilename, &wg, nodeIds,myChan,2)
    }

    wg.Wait()
    for i:= 0;i<4;i++{
        newVal := <-myChan
        if newVal < 0 {
            continue
        } else {
            LocdoneList = append(LocdoneList,newVal)
        }
    }

    doneList_str := list2String(LocdoneList)
    if len(LocdoneList) > 0{
        log.Printf("[INSIDE PUTFILEWRAPPER] success putting %s %s\n", localFilename, sdfsFilename)
        fmt.Printf("Send ack to master")
        sendAcktoMaster("put", myVid, doneList_str, sdfsFilename)
    } else {
        fmt.Printf("Can't Finish Put request for %d\n",sdfsFilename)
    }

    // elapsed := time.Since(initTime)

    // fmt.Printf("Time taken for put %s\n",elapsed)
}

func sendKeyFile(action string, srcNode int, taskId int, keysFilename string) {
    message := fmt.Sprintf("keyfile %s %d %d", action, srcNode, taskId)
    padded_message := fillString(message, messageLength)

    timeout := time.Duration(20) * time.Second


    acquireConn()
    conn, err := net.DialTimeout("tcp", masterIP + ":" + strconv.Itoa(fileTransferPort), timeout)
    if err != nil {
        log.Printf("[ME %d] Unable to connect with the master ip=%s port=%d", myVid, masterIP, masterPort)
        releaseConn()
        return
    }

    conn.Write([]byte(padded_message))
    simpleSendFile(conn, keysFilename)
    fmt.Printf("Sent all the keys to the master\n")

    conn.Close()
    releaseConn()
}

func simpleSendFile(conn net.Conn, filename string) {
    acquireFile()
    activeFileNum = activeFileNum+1
    fmt.Printf("The number of active Files %d \n",activeFileNum)
    f, err := os.Open(filename)
    if err != nil {
        fmt.Printf("Cannot open %s\n", filename)
        activeFileNum = activeFileNum-1
        fmt.Printf("The number of active Files %d \n",activeFileNum)
        // f.Close()
        releaseFile()
        return
    }
    defer releaseFile()
    defer f.Close()


    fileInfo, err := f.Stat()
    if err != nil {
        fmt.Printf("Can't access file stats for %s\n", filename)
        activeFileNum = activeFileNum-1
        fmt.Printf("The number of active Files %d \n",activeFileNum)
        return
    }


    fileSize := fillString(strconv.FormatInt(fileInfo.Size(), 10), 10)
    if fileInfo.Size() == 0 {
        fmt.Printf("=======********** , filesize = 0, %s\n", filename)
    }
    
    fileName := fillString(fileInfo.Name(), 64)

    fmt.Printf("Original filename parameter %s, Filesize %d, Filename %s\n",  filename, fileSize, fileName)


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
    activeFileNum = activeFileNum-1
    fmt.Printf("The number of active Files %d \n",activeFileNum)
    return
}

// fectches to the maple dir!!
func simpleRecvFile(conn net.Conn) string {
    bufferFileSize := make([]byte, 10)
    bufferFileName := make([]byte, 64)

    conn.Read(bufferFileSize)
    fmt.Printf("raw file size: blah-%s-blah, length %d\n", string(bufferFileSize),len(string(bufferFileSize)))

    conn.Read(bufferFileName)
    fmt.Printf("raw file name: blah-%s-blah ,length %d\n", string(bufferFileName), len(string(bufferFileName)))

    if !strings.Contains(string(bufferFileName),"^") {
        fmt.Printf("didnot read anythingas filename or size\n")
        log.Printf("didnot read anythingas filename or size\n")

        return ""
    }
    fileSize, _ := strconv.ParseInt(strings.Trim(string(bufferFileSize), filler), 10, 64)
    fileName := strings.Trim(string(bufferFileName), filler)


    fmt.Printf("Incoming filesize %d filename %s\n", fileSize, fileName)

    mapleFilePath := maple_dir + fileName
    acquireFile()
    activeFileNum = activeFileNum+1
    fmt.Printf("The number of active Files %d \n",activeFileNum)
    f, err := os.Create(mapleFilePath)
    if err != nil {
        fmt.Printf("Cannot create file %s\n", mapleFilePath) 
        // If this happens
        releaseFile()
        fmt.Println(err)
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
    // activeFileNum = activeFileNum-1
    // fmt.Printf("The number of active Files %d \n",activeFileNum)
    releaseFile()
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

    // run_cmd := fmt.Sprintf("./%s -inputfile %s", exeFile, inputFilePath)
    run_cmd := fmt.Sprintf("cat %s | python %s",inputFilePath,exeFile)
    fmt.Printf("Trying to run %s\n", run_cmd)
    acquireFile()
    // activeFileNum = activeFileNum+1
    // fmt.Printf("The number of active Files %d \n",activeFileNum)
    outfile, err := os.Create(outputFilePath)
    cmd := exec.Command("sh","-c", run_cmd)
    // outfile, err := os.Create(outputFilePath)
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
    // activeFileNum = activeFileNum-1
    // fmt.Printf("The number of active Files %d \n",activeFileNum)
    releaseFile()

    fmt.Printf("Maple execution done\n")

    // separate output into key specific files
    // todo: extend to more than 1024 keys!!
    acquireFile()
    // activeFileNum = activeFileNum+1
    // fmt.Printf("The number of active Files %d \n",activeFileNum)
    f, err := os.Open(outputFilePath)
    if err != nil {
        fmt.Printf("Cannot open %s for reading\n", outputFilePath)
        releaseFile()
        return
    }
    fmt.Printf("opened generated output file for key separation\n")

    keyFileHandleMap := make(map[string]*os.File)
    keySet := make(map[string]bool)

    fileReader := bufio.NewReader(f)
    fileEnd := false
    for {
        line, err := fileReader.ReadString('\n')
        if err != nil {
            if err != io.EOF {
                fmt.Printf("Unknown error encountered while reading file\n")
                // f.Close()
                // <-newguard
                fileEnd = true
                break
            }
            fileEnd = true
            // break
        }
        if len(line) > 0 {
            // fmt.Printf("curr line: %s", line)
            splitLine := strings.Split(line, " ")
            key := splitLine[0]
            keySet[key] = true
            _, ok := keyFileHandleMap[key]
            if ok {
                temp := keyFileHandleMap[key]
                temp.WriteString(line)
            } else {
                tempFilePath := fmt.Sprintf(maple_dir + "output_%d_%s.out", mapleId, key)

                if len(keyFileHandleMap) > 128 {
                    var randomkey string
                    for randomkey = range keyFileHandleMap {
                        break
                    }
                    keyFileHandleMap[randomkey].Close()
                    releaseFile()
                    delete(keyFileHandleMap, randomkey)
                }
                acquireFile()
                tempFile, err := os.OpenFile(tempFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
                if err != nil {
                    fmt.Printf("Could not append/create the file %s\n", tempFilePath)
                    panic(err)
                } 
            // fmt.Printf("created new file handler for %s\n", tempFilePath)
                keyFileHandleMap[key] = tempFile
                tempFile.WriteString(line)
                // tempFile.Close()
            }
        }
        if fileEnd {
            for _, fhandle := range(keyFileHandleMap) {
                fhandle.Close()
                releaseFile()
            }
            f.Close()
            // activeFileNum = activeFileNum-1
            // fmt.Printf("The number of active Files %d \n",activeFileNum)
            fileEnd = false
            releaseFile()
            break
        }
        
    }
    if fileEnd {
        for _, fhandle := range(keyFileHandleMap) {
            fhandle.Close()
            releaseFile()
        }
        f.Close()
        activeFileNum = activeFileNum-1
        fmt.Printf("The number of active Files %d \n",activeFileNum)
        releaseFile()
        // break
    }
    
    keysFilename := fmt.Sprintf("keys_%d.info", mapleId)
    acquireFile()
    keysFileHandle, err := os.Create(keysFilename)
    if err != nil {
        fmt.Printf("Could not create %s\n", keysFilename)
        releaseFile()
        panic(err)
    }
    // may Add a dummy key 
    for key := range(keySet) {
        keysFileHandle.WriteString(key + "$$$$")
    }
    keysFileHandle.Close()
    releaseFile()
    fmt.Printf("Key File : %s created\n",keysFilename)

    sendKeyFile("maple", myVid, mapleId, keysFilename)
}

// this is suboptimal as in case of failures, we will read all the mapleId key files
func AssembleKeyFiles() {

    // This part is done by master
    if len(keyMapleIdMap) == 0 {
        for mapleId := range mapleId2Node {
            keysFilename := fmt.Sprintf("keys_%d.info", mapleId)
            acquireFile()
            // activeFileNum = activeFileNum+1
            // fmt.Printf("The number of active Files %d \n",activeFileNum)
            // Modify
            fileOpen,err:= os.Open(maple_dir + keysFilename)
            if err != nil {
                log.Panicf("failed reading file: %s", err)
                // fileOpen.Close()
                // activeFileNum = activeFileNum-1
                // fmt.Printf("The number of active Files %d \n",activeFileNum)
                releaseFile()
                panic(err)
            }
            contentBytes, err := ioutil.ReadAll(fileOpen)
            fileOpen.Close()
            // activeFileNum = activeFileNum-1
            // fmt.Printf("The number of active Files %d \n",activeFileNum)
            releaseFile()
            if err != nil {
                fmt.Printf("Could not read file corresponding to %d maple id\n", mapleId)
                panic(err)
            }
            content := string(contentBytes)
            keys := strings.Split(content, "$$$$")
            keys = keys[:len(keys)-1]

            currNode := mapleId2Node[mapleId]
            node2mapleJob[currNode].keysGenerate = append(node2mapleJob[currNode].keysGenerate, keys...)

            for _, key := range keys {
                keyDone[key] = false
                _, ok := keyMapleIdMap[key]
                if ok {
                    keyMapleIdMap[key] = append(keyMapleIdMap[key], mapleId)
                } else {
                    keyMapleIdMap[key] = []int{mapleId}
                }
            }
            // fmt.Printf("Keys for %d maple id: %v, len of keys = %d\n", mapleId, keys, len(keys))
            
        }
        // fmt.Printf("%v\n", keyMapleIdMap)
        keyCount = len(keyMapleIdMap)
    }
    
    // assign key processing to worker nodes
    // do range or hash partitioning
    nodeIdx := 0
    
    // testguard = make(chan struct{}, 64) // limitng the number
    
    for key := range keyMapleIdMap {
        _, ok := keyStatus[key]
        if ok && (keyStatus[key] == DONE || keyStatus[key] == ONGOING) {
            continue
        }
        currNode := workerNodes[nodeIdx]
        // fmt.Printf("Accessing node in Assemple Key : %d \n",currNode)
        // log.Printf("Accessing node in Assemple Key : %d \n",currNode)

        // fmt.Printf("Accessing node in Assemple Key : %d keyAggr thing %v\n",currNode, node2mapleJob[currNode].keysAggregate)

        // log.Printf("Accessing node in Assemple Key : %d keyAggr thing %v\n",currNode, node2mapleJob[currNode].keysAggregate)

        node2mapleJob[currNode].keysAggregate = append(node2mapleJob[currNode].keysAggregate, key)

        // go ProcessKey(key, currNode, keyMapleIdMap[key])
        acquireParallel() // would block if guard channel is already filled
        go func(key string, respNode int, mapleIds []int) {
            ProcessKey(key, respNode, mapleIds)
            releaseParallel()
        }(key, currNode, keyMapleIdMap[key])
    
        nodeIdx = (nodeIdx + 1) % len(workerNodes)
    }

    // releaseConn()
}

func ProcessKey(key string, respNode int, mapleIds []int) {
    keyStatus[key] = ONGOING

    // Ensure only some work is given per node

    // newguard <- struct{}{}
    fmt.Printf("Inside process key: key %s, respNode %d, maple ids that have this key: %v\n", key, respNode, mapleIds)

    keysFilename := fmt.Sprintf("%s%s_node.info",maple_dir,key)
    
    acquireFile()
    // activeFileNum = activeFileNum+1
    // fmt.Printf("The number of active Files %d \n",activeFileNum)
    keyfile, err := os.Create(keysFilename)
    if err != nil {
        fmt.Printf("Could not create %s file\n", keysFilename)
        releaseFile()
        // keyStatus[key] = FAILED
        return
    }

    for _, mapleId := range mapleIds {
        // each record is mapleId:nodeId
        line := fmt.Sprintf("%d:%d", mapleId, mapleId2Node[mapleId])
        keyfile.WriteString(line + "$$$$")
    } 
    keyfile.Close()
    // activeFileNum = activeFileNum-1
    // fmt.Printf("The number of active Files %d \n",activeFileNum)
    releaseFile()
    timeout := time.Duration(20) * time.Second

    nodeIP := memberMap[respNode].ip
    acquireConn()
    conn, err := net.DialTimeout("tcp", nodeIP + ":" + strconv.Itoa(fileTransferPort), timeout)
    if err != nil {
        log.Printf("[ME %d] Unable to connect with the master ip=%s port=%d", myVid, masterIP, masterPort)
        releaseConn()
        return
    }

    message := fmt.Sprintf("keyaggr %s %s %s %d", key, sdfsInterPrefix,keysFilename,myVid) // send filepath 
    padded_message := fillString(message, messageLength)

    conn.Write([]byte(padded_message))
    // simpleSendFile(conn, keysFilename) // Don't use this
    conn.Close()
    releaseConn()

    keyTimeStamp[key] = time.Now().Unix()
}


func KeyAggregation(key string, nodeInfoList []string) {
    // var wg sync.WaitGroup
    // wg.Add(len(nodeInfoList))
    acquireParallel()
    defer releaseParallel()
    ch := make(chan bool, len(nodeInfoList))

    dataFileList := []string{}

    fmt.Printf("[conn explosion check] before *KEYAGGR* %s loop  %d\n", key, activeConnCount)
    log.Printf("[conn explosion check] before *KEYAGGR* %s loop  %d\n", key, activeConnCount)


    for _, nodeInfo := range(nodeInfoList) {
         
        splitNodeInfo:= strings.Split(nodeInfo, ":")
        mapleId_str := splitNodeInfo[0]
        nodeId_str := splitNodeInfo[1]
        nodeId, _ := strconv.Atoi(nodeId_str)
        dataFilePath := fmt.Sprintf("%soutput_%s_%s.out", maple_dir, mapleId_str, key)
        dataFileList = append(dataFileList, dataFilePath)
        // connguard <- struct{}{}
        go getDirFile(nodeId, dataFilePath, dataFilePath, ch, 3)
    }
    
    for i := 0; i < len(nodeInfoList); i++ {
        success := <- ch
        if !success {
            // releaseConn()
            return
        }
    }

    fmt.Printf("[conn explosion check] after *KEYAGGR* %s loop  %d\n", key, activeConnCount)
    log.Printf("[conn explosion check] after *KEYAGGR* %s loop  %d\n", key, activeConnCount)

    fmt.Printf("Got all files for key %s aggregation\n", key)

    outFilename := fmt.Sprintf("%s_inter.info", key)
    outFilePath := fmt.Sprintf("%s%s", local_dir, outFilename)
    
    statusUpdate:=AppendFiles(dataFileList, outFilePath,2)
    if !statusUpdate{
        fmt.Printf("Unable to append file for key %s \n",key)
        return
    }



    // timeout := 20 * time.Second
    // acquireConn()
    // conn, err := net.DialTimeout("tcp", masterIP + ":" + strconv.Itoa(masterPort), timeout)
    // if err != nil {
    //     log.Printf("[ME %d] Unable to connect with the master ip=%s port=%d", myVid, masterIP, masterPort)
    //     releaseConn()
    //     return
    // }

    // PutFileWrapper(outFilename, sdfsInterPrefix + "_" + key, conn)
    // conn.Close()
    // releaseConn()
    valSuc := PutFileWrapper2(outFilename, sdfsInterPrefix + "_" + key)
    if !valSuc{
        valSuc = PutFileWrapper2(outFilename, sdfsInterPrefix + "_" + key)
    }

    if valSuc {
        acquireConn()

        timeout := 20 * time.Second


        conn, err := net.DialTimeout("tcp", masterIP + ":" + strconv.Itoa(mapleJuicePort), timeout)
          if err != nil {
            log.Printf("[ME %d] Unable to connect with the master ip=%s port=%d", myVid, masterIP, mapleJuicePort)
            fmt.Printf("[ME %d] Unable to connect with the master ip=%s port=%d", myVid, masterIP, mapleJuicePort)

            // should not close the connection if err-ed; gives seg fault
            // conn.Close()
            releaseConn()
            return
        }
        message := fmt.Sprintf("keyack %s %d\n", key,myVid)
        fmt.Printf("Sending %s\n",message)
        log.Printf("Sending %s\n",message)
        fmt.Fprintf(conn, message)
        conn.Close()
        releaseConn()

        fmt.Printf(" ========************======== sENT KEYACK for %s\n", key)
        fmt.Printf("Appended the file for %s key\n", key)
        return
    }

    // send an ack to the master
    
    // put the appended file into sdfs and notify master
}

func getDirFile(destNodeId int, destFilePath string, localFilePath string, ch chan<- bool, count int) {

    // get file 
    if count <= 0 {
        ch <- false
        return
    }

    if destNodeId == myVid {
        success := copyFile(destFilePath, localFilePath)
        ch <- success
        // if success {
        //     doneList = append(doneList, nodeId)
        //     fmt.Printf("sent the file to %d\n", nodeId)
        //     wg.Done()
        // }

        return
    }

    if !memberMap[destNodeId].alive {
        ch <- false
        return
    }

    timeout := time.Duration(20) * time.Second

    ip := memberMap[destNodeId].ip
    port := fileTransferPort

    acquireConn()
    conn, err := net.DialTimeout("tcp", ip + ":" + strconv.Itoa(port), timeout) 
    if err != nil {
        log.Printf("[ME %d] Unable to dial a connection to %d (to get file %s)\n", myVid, destNodeId, destFilePath)
        // ch <- false
        // should not close the connection if err-ed; gives seg fault
        // conn.Close()
        releaseConn()
        // connguard <- struct{}{}

        time.Sleep(10 * time.Millisecond)

        go getDirFile(destNodeId,destFilePath,localFilePath,ch,count-1)
        return
    }
    defer releaseConn()
    defer conn.Close()

    message := fmt.Sprintf("getfile2 %s", destFilePath)
    padded_message := fillString(message, messageLength)
    conn.Write([]byte(padded_message))

    bufferFileSize := make([]byte, 10)

    _, err = conn.Read(bufferFileSize)
    if err != nil {
        fmt.Println(err) // what error are you getting?
        log.Printf("[ME %d] Error while fetching file %s from %d\n", myVid, destFilePath, destNodeId)
        fmt.Printf("[ME %d] Error while fetching file %s from %d\n", myVid, destFilePath, destNodeId)
        // ch <- false
        // conn.Close()
        // releaseConn()
        // connguard <- struct{}{}
        time.Sleep(10 * time.Millisecond)
        go getDirFile(destNodeId, destFilePath, localFilePath, ch, count-1)
        return
    }

    fileSize, _ := strconv.ParseInt(strings.Trim(string(bufferFileSize), filler), 10, 64)
    log.Printf("[ME %d] Incoming file size %d", myVid, fileSize)

    // fmt.Printf("[ME %d] Incoming file size %d", myVid, fileSize)
    acquireFile()
    // activeFileNum = activeFileNum+1
    // fmt.Printf("The number of active Files %d \n",activeFileNum)
    file, err := os.Create(localFilePath)
    if err != nil {
        log.Printf("[ME %d] Cannot create the local file %s", myVid, localFilePath) 
        // ch <- false
        // activeFileNum = activeFileNum-1
        // fmt.Printf("The number of active Files %d \n",activeFileNum)
        // conn.Close()
        releaseFile()
        // releaseConn()
        // connguard <- struct{}{}
        time.Sleep(10 * time.Millisecond)
        go getDirFile(destNodeId,destFilePath,localFilePath,ch,count-1)
        return
    }
    defer releaseFile()
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
        fmt.Printf("Received file %s\n", destFilePath)
    } else {
        time.Sleep(10 * time.Millisecond)
        go getDirFile(destNodeId,destFilePath,localFilePath,ch,count-1)
        return
    }

    ch <- success
    // file.Close()
    // activeFileNum = activeFileNum-1
    // fmt.Printf("The number of active Files %d \n",activeFileNum)
    // conn.Close()
    // releaseGuard()
    // releaseConn()
    return    
}

func AppendFiles(inputFilePaths []string, outFilePath string, tryCount int)(bool) {

    if tryCount<=0{
        return false
    }
    acquireFile()
    // activeFileNum = activeFileNum+1
    // fmt.Printf("The number of active Files %d \n",activeFileNum)
    outfile, err := os.Create(outFilePath)
    if err != nil {
        fmt.Printf("Could not open %s file for appending\n", outFilePath)
        releaseFile()
        return AppendFiles(inputFilePaths,outFilePath,tryCount-1)
        // panic(err)
    }

    for _, inputfile := range inputFilePaths {
        acquireFile() 
        // activeFileNum = activeFileNum+1
        // fmt.Printf("The number of active Files %d \n",activeFileNum)
        fIn, err := os.Open(inputfile)
        if err != nil {
            fmt.Printf("Could not open %s file for appending\n", inputfile)
            releaseFile()
            releaseFile()// two files
            return AppendFiles(inputFilePaths,outFilePath,tryCount-1)
            // panic(err)
        } 

        _, err = io.Copy(outfile, fIn)
        if err != nil {
            fmt.Printf("Could not copy the file from %s to %s\n", inputfile, outFilePath)
            releaseFile()
            releaseFile()// two files
            return AppendFiles(inputFilePaths,outFilePath,tryCount-1)
        }
        fIn.Close()
        // activeFileNum = activeFileNum-1
        // fmt.Printf("The number of active Files %d \n",activeFileNum)
        releaseFile()
    }

    outfile.Close()
    // activeFileNum = activeFileNum-1
    // fmt.Printf("The number of active Files %d \n",activeFileNum)
    
    releaseFile()
    fmt.Printf("Merged all files %v\n", inputFilePaths)
    return true


}


func getAliveNodeCount() int {
    aliveNodeCount := 0
    for node := range memberMap {
        if memberMap[node].alive {
            aliveNodeCount += 1
        }
    }
    return aliveNodeCount
}

func min(a int, b int) int {
    if a < b {
        return a
    } else {
        return b
    }
}

func removeFromList(l []int, target int)([]int) {
    targetidx := -1
    for idx, elem := range l {
        if elem == target {
            targetidx = idx
            break
        }
    }
    if targetidx != -1 {
        l[targetidx] = l[len(l)-1]
        l = l[:len(l)-1]
    }
    return l
    
}



func handleJuiceFaiure(subject int){
    log.Printf("handling JUICE FALIURE for %d juiceRunning: %v\n", subject, juiceRunning)
    fmt.Printf("handling JUICE FALIURE for %d juiceRunning: %v\n", subject, juiceRunning)
    if juiceRunning{
        _, isNodeJuice := node2juiceJob[subject]
            // replacement := getRandomNodes(append(workerNodes, 0), 1)[0]

        if isNodeJuice{
            // workerNodes = removeFromList(workerNodes, subject)
            replacement := getRandomNodes(append(workerNodes, 0), 1)[0]
            for i:=0;i<4;i++{
                if replacement!=0{
                    break
                }
                replacement = getRandomNodes(append(workerNodes, 0), 1)[0]
                
            }

            if replacement == 0{
                return
            }

            workerNodes = removeFromList(workerNodes, subject)

            var jobnode juiceJob
            jobnode.assignedJuiceIds = node2juiceJob[subject].assignedJuiceIds
            // jobnode.keysAggregate = node2juiceJob[subject].keysAggregate
            node2juiceJob[replacement] = &jobnode
            workerNodes = append(workerNodes, replacement)
            fmt.Printf("[ME %d] Juice Worker node %v \n",myVid,workerNodes)
            log.Printf("[ME %d] Juice Worker node %v \n",myVid,workerNodes)

            asignedIds := node2juiceJob[replacement].assignedJuiceIds
            for _, juiceid := range(asignedIds){
                if juiceCompMap[juiceid] == DONE{
                    continue
                }
                juiceId2Node[juiceid] = replacement
                juiceCompMap[juiceid] = ONGOING
                juiceTimeStamp[juiceid] = time.Now().Unix()
                log.Printf("Sent SENTJUICE keyFile : %s to nodeID : %d with juiceID : %d\n",juiceFiles[juiceid],replacement,juiceid)

                sendJuiceInfo(replacement,juiceid,sdfsJuiceExe,juiceFiles[juiceid])

            }

        }
    }

    return
}


func handleMapleFailure(subject int) {

    log.Printf("handling MAPLE FALIURE for %d mapleRunning: %v\n", subject, mapleRunning)

    if mapleRunning {
        _, isNodeMaple := node2mapleJob[subject]
        if isNodeMaple {
            fmt.Printf("Rerunning the maple task for failed nodes %d \n",subject)
            log.Printf("Rerunning the maple task for failed nodes %d \n",subject)

            // this node is running maple

            // [TODO] what is the system does not have enough nodes to satisfy this req, handle that
            replacement := getRandomNodes(append(workerNodes, 0), 1)[0]
            for i:=0; i<4;i++{
                
                if replacement!=0{
                    break
                }
                replacement = getRandomNodes(append(workerNodes, 0), 1)[0]

            }
            if replacement == 0{
                fmt.Printf("****************************************************")
                fmt.Printf("********* CANT HANDLE MAPLE FAIL *******************")
                fmt.Printf("****************************************************")
                log.Printf("****************************************************")
                log.Printf("********* CANT HANDLE MAPLE FAIL *******************")
                log.Printf("****************************************************")
                return
            }
            workerNodes = removeFromList(workerNodes, subject)

            // workerNodes = removeFromList(workerNodes, subject)
            var jobnode mapleJob
            jobnode.assignedMapleIds = node2mapleJob[subject].assignedMapleIds
            jobnode.keysAggregate = node2mapleJob[subject].keysAggregate
            jobnode.keysGenerate = node2mapleJob[subject].keysGenerate
            node2mapleJob[replacement] = &jobnode

            workerNodes = removeFromList(workerNodes, subject)
            workerNodes = append(workerNodes, replacement)
            fmt.Printf("[ME %d] Worker node %v \n",myVid,workerNodes)
            log.Printf("[ME %d] Worker node %v \n",myVid,workerNodes)

            if !mapleBarrier {
                // re-run all the maple ids assigned to this node
                assignedMapleIds := node2mapleJob[replacement].assignedMapleIds
                for _, mapleid := range(assignedMapleIds) {
                    mapleId2Node[mapleid] = replacement
                    mapleCompMap[mapleid] = false
                    // connguard <- struct{}{}   
                    acquireParallel() // would block if guard channel is already filled
                    go func(node int, mapleID int, fname string, inpFile string) {
                        sendMapleInfo(node, mapleID, fname,inpFile)
                        releaseParallel()
                    }(replacement, mapleid, sdfsMapleExe, mapleFiles[mapleid])          
                    // go sendMapleInfo(replacement, mapleid, sdfsMapleExe, mapleFiles[mapleid])
                }
                return
            } else {
                mapleAgain := false
                for _, keyGen := range node2mapleJob[subject].keysGenerate {
                    if keyStatus[keyGen] != DONE {
                        mapleAgain = true
                        break
                    }
                }

                if mapleAgain {
                    for _, keyGen := range node2mapleJob[subject].keysGenerate {
                        if keyStatus[keyGen] != DONE{
                            keyStatus[keyGen] = FAILED
                        }
                        
                    }
                    for _, keyAggr := range node2mapleJob[subject].keysAggregate {
                        if keyStatus[keyAggr] != DONE{
                            keyStatus[keyAggr] = FAILED
                        }
                        
                    }
                    mapleBarrier = false
                    assignedMapleIds := node2mapleJob[replacement].assignedMapleIds
                    for _, mapleid := range(assignedMapleIds) {
                        mapleId2Node[mapleid] = replacement
                        mapleCompMap[mapleid] = false

                        // connguard <- struct{}{}
                        acquireParallel() // would block if guard channel is already filled
                        go func(node int, mapleID int, fname string, inpFile string) {
                            sendMapleInfo(node, mapleID, fname,inpFile)
                            releaseParallel()
                        }(replacement, mapleid, sdfsMapleExe, mapleFiles[mapleid])
                        // go sendMapleInfo(replacement, mapleid, sdfsMapleExe, mapleFiles[mapleid])
                    }
                } else{
                    // testguard := make(chan struct{}, 50)
                    for _, keyAggr := range node2mapleJob[subject].keysAggregate {
                        if keyStatus[keyAggr] != DONE {
                            keyStatus[keyAggr] = FAILED
                            acquireParallel() // would block if guard channel is already filled
                            go func(key string, respNode int, mapleIds []int) {
                                ProcessKey(key, respNode, mapleIds)
                                releaseParallel()
                            }(keyAggr, replacement, keyMapleIdMap[keyAggr])
                            // go ProcessKey(keyAggr, replacement, keyMapleIdMap[keyAggr])
                        }  
                    } 
                }
            }
            return
        }
    }
}

func deleteMap(m map[interface{}]interface{}) {
    for k := range m {
        delete(m, k)
    } 
}



func JuiceProcess(){
    log.Printf("[ME %d] Started the Juice Process",myVid)
    fmt.Printf("[ME %d] Started the Juice Process",myVid)
    for{
        <- juiceToken
        process := false
        mutex.Lock()
        log.Printf("[ME %d] Got the LOCK Juice Process",myVid)
        fmt.Printf("[ME %d] Got the LOCK Juice Process",myVid)
        var juiceKey string
        if len(JuiceQueue) > 0{
            juiceKey = JuiceQueue[0]
            JuiceQueue = JuiceQueue[1:]
            process = true
        }
        mutex.Unlock()
        if process{
            // Do the execute thing
            // acquireParallel()
            go func (juiceKey string) {
                juiceVal := strings.Split(juiceKey," ")
                ExecuteJuice(juiceVal[0],juiceVal[1],juiceVal[2],juiceVal[3])
                // releaseParallel() 
            }(juiceKey)
        }

    }
}


func ExecuteJuice(exeFile string, inputFilePath string, outputFilePath string, juiceID string) {

    fmt.Printf("Inside the execute Juice for %s %s %s %s\n",exeFile,inputFilePath,outputFilePath,juiceID)
    err := os.Chmod(exeFile, 0777)
    if err != nil {
        fmt.Printf("%v\n", err)
    }

    if !getFileWrapper(inputFilePath, inputFilePath){
        getFileWrapper(inputFilePath, inputFilePath)
    }   


    // run_cmd := fmt.Sprintf("./%s -inputfile %s%s", exeFile, local_dir,inputFilePath)
    run_cmd := fmt.Sprintf("cat %s%s | python %s",local_dir,inputFilePath,exeFile)
    fmt.Printf("Trying to run %s\n", run_cmd)
    // acquireFile()
    // activeFileNum = activeFileNum+1
    // fmt.Printf("The number of active Files %d \n",activeFileNum)
    // outfile, err := os.Create(outputFilePath)
    out_cmd,err := exec.Command("sh","-c", run_cmd).Output()
    // outfile, err := os.Create(outputFilePath)
    if err != nil {
        fmt.Printf("%v\n", err)
        log.Printf("[Me %d]  Can't run juice\n",myVid)
        // panic(err)
    }

    out_cmdStr := string(out_cmd)
    fmt.Printf(" Output of Juice is %s \n",out_cmdStr)


    fmt.Printf("[ME %d] Juice execution done for %s and file %s \n",myVid,juiceID,out_cmdStr)
    log.Printf("[ME %d] Juice execution done for %s and file %s \n",myVid,juiceID,out_cmdStr)

    // Send Key ack

    acquireConn()

    timeout := 20 * time.Second


    conn, err := net.DialTimeout("tcp", masterIP + ":" + strconv.Itoa(mapleJuicePort), timeout)
      if err != nil {
        log.Printf("[ME %d] Unable to connect with the master ip=%s port=%d", myVid, masterIP, mapleJuicePort)
        fmt.Printf("[ME %d] Unable to connect with the master ip=%s port=%d", myVid, masterIP, mapleJuicePort)

        // should not close the connection if err-ed; gives seg fault
        // conn.Close()
        releaseConn()
        return
    }

    // out_cmdStr = strings.Replace(out_cmdStr," ", "-", -1)

    message := fmt.Sprintf("keyJuice %s %d %s\n", juiceID , myVid, out_cmdStr)

    fmt.Printf("Sending %s\n",message)
    log.Printf("Sending %s\n",message)
    fmt.Fprintf(conn, message)
    conn.Close()
    releaseConn()

    fmt.Printf(" ========************======== JUICE KEYACK for %s\n", juiceID)
    // fmt.Printf("Appended the file for %s key\n", out_cmd)

    // separate output into key specific files
    // todo: extend to more than 1024 keys!!
    
}


