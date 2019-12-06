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


func sendMapleInfo(nodeId int, mapleId int, sdfsMapleExe string, inputFile string) {
	timeout := 20 * time.Second

    ip := memberMap[nodeId].ip
    port := fileTransferPort

    conn, err := net.DialTimeout("tcp", ip + ":" + strconv.Itoa(port), timeout) 
    if err != nil {
        log.Printf("[ME %d] Unable to dial a connection to %d (to send maple task for %s)\n", myVid, nodeId, sdfsMapleExe)
        return
    }
    defer conn.Close()

    message := fmt.Sprintf("runmaple %d %s %s", mapleId, sdfsMapleExe, inputFile)
    fmt.Printf("%s\n", message)
    padded_message := fillString(message, messageLength)
    conn.Write([]byte(padded_message))
    select {
        case msg := <-connguard:
            fmt.Println("End connguard message %v \n", msg)
        default:
            fmt.Println("No message received\n")
    }
}

func getFileWrapper(sdfsFilename string, localFilename string) bool {
    fmt.Printf("Inside get file wrapper\n")
    initTime := time.Now()
    // sdfsFilename := split_command[1]
    // localFilename := split_command[2]
    // Create a connection to main to ask for the file
	timeout := 20 * time.Second
    conn, err := net.DialTimeout("tcp", masterIP + ":" + strconv.Itoa(masterPort), timeout)
    if err != nil {
        log.Printf("[ME %d] Unable to connect with the master ip=%s port=%d", myVid, masterIP, masterPort)
        return false
    }


    master_command := fmt.Sprintf("get %s\n", sdfsFilename)
    fmt.Fprintf(conn, master_command) // get the running 

    fmt.Printf("sent a get request %s to the master\n", master_command) 


    reader := bufio.NewReader(conn)
    reply, err := reader.ReadString('\n')
    if err != nil || len(reply) == 0 {
        log.Printf("[ME %d] Could not read reply from master (for get %s)\n", myVid, sdfsFilename)
        conn.Close()
        return false
    }

    conn.Close()    // [NEW]
    if len(reply) == 0{
        fmt.Printf("Empty reply received fot file %s\n",sdfsFilename)
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
        // return false
    }

    fmt.Printf("Time taken for get %s\n", elapsed)
    return success

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
        connguard <- struct{}{}
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
    newguard <- struct{}{}
    activeFileNum = activeFileNum+1
    fmt.Printf("The number of active Files %d \n",activeFileNum)
    f, err := os.Open(filename)
    if err != nil {
        fmt.Printf("Cannot open %s\n", filename)
        activeFileNum = activeFileNum-1
        fmt.Printf("The number of active Files %d \n",activeFileNum)
        <-newguard
        return
    }
    defer f.Close()

    fileInfo, err := f.Stat()
    if err != nil {
        fmt.Printf("Can't access file stats for %s\n", filename)
        activeFileNum = activeFileNum-1
        fmt.Printf("The number of active Files %d \n",activeFileNum)
        <-newguard
        return
    }


    fileSize := fillString(strconv.FormatInt(fileInfo.Size(), 10), 10)
    if fileInfo.Size() == 0 {
        fmt.Printf("=======********** DAMN, filesize = 0, %s\n", filename)
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
    <-newguard
    return
}

// fectches to the maple dir!!
func simpleRecvFile(conn net.Conn) string {
    bufferFileSize := make([]byte, 10)
    bufferFileName := make([]byte, 64)

    conn.Read(bufferFileSize)
    fmt.Printf("raw file size: %s\n", string(bufferFileSize))
    fileSize, _ := strconv.ParseInt(strings.Trim(string(bufferFileSize), filler), 10, 64)

    conn.Read(bufferFileName)
    fmt.Printf("raw file name: %s\n", string(bufferFileName))
    fileName := strings.Trim(string(bufferFileName), filler)

    fmt.Printf("Incoming filesize %d filename %s\n", fileSize, fileName)

    mapleFilePath := maple_dir + fileName
    newguard <- struct{}{}
    activeFileNum = activeFileNum+1
    fmt.Printf("The number of active Files %d \n",activeFileNum)
    f, err := os.Create(mapleFilePath)
    if err != nil {
        fmt.Printf("Cannot create file %s\n", mapleFilePath) 
        // If this happens
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
    activeFileNum = activeFileNum-1
    fmt.Printf("The number of active Files %d \n",activeFileNum)
    <-newguard
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
    newguard <- struct{}{}
    activeFileNum = activeFileNum+1
    fmt.Printf("The number of active Files %d \n",activeFileNum)
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
    activeFileNum = activeFileNum-1
    fmt.Printf("The number of active Files %d \n",activeFileNum)
    <-newguard 

    fmt.Printf("Maple execution done\n")

    // separate output into key specific files
    // todo: extend to more than 1024 keys!!
    newguard <- struct{}{}
    activeFileNum = activeFileNum+1
    fmt.Printf("The number of active Files %d \n",activeFileNum)
    f, err := os.Open(outputFilePath)
    if err != nil {
        fmt.Printf("Cannot open %s for reading\n", outputFilePath)
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
                    delete(keyFileHandleMap, randomkey)
                }

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
            }
            f.Close()
            activeFileNum = activeFileNum-1
            fmt.Printf("The number of active Files %d \n",activeFileNum)
            fileEnd = false
            <-newguard 
            break
        }
        
    }
    if fileEnd {
            for _, fhandle := range(keyFileHandleMap) {
                fhandle.Close()
            }
            f.Close()
            activeFileNum = activeFileNum-1
            fmt.Printf("The number of active Files %d \n",activeFileNum)
            <-newguard 
            // break
    }
    
    keysFilename := fmt.Sprintf("keys_%d.info", mapleId)
    keysFileHandle, err := os.Create(keysFilename)
    if err != nil {
        fmt.Printf("Could not create %s\n", keysFilename)
        panic(err)
    }
    // may Add a dummy key 
    for key := range(keySet) {
        keysFileHandle.WriteString(key + "$$$$")
    }
    keysFileHandle.Close()
    fmt.Printf("Key File : %s created\n",keysFilename)

    sendKeyFile("maple", myVid, mapleId, keysFilename)
}

// this is suboptimal as in case of failures, we will read all the mapleId key files
func AssembleKeyFiles() {

    // This part is done by master
    if len(keyMapleIdMap) == 0 {
        for mapleId := range mapleId2Node {
            keysFilename := fmt.Sprintf("keys_%d.info", mapleId)
            newguard <- struct{}{}
            activeFileNum = activeFileNum+1
            fmt.Printf("The number of active Files %d \n",activeFileNum)
            contentBytes, err := ioutil.ReadFile(maple_dir + keysFilename)
            activeFileNum = activeFileNum-1
            fmt.Printf("The number of active Files %d \n",activeFileNum)
            <-newguard
            if err != nil {
                fmt.Printf("Could not read file corresponding to %d maple id\n", mapleId)
                panic(err)
            }
            content := string(contentBytes)
            keys := strings.Split(content, "$$$$")
            keys = keys[:len(keys)-1]

            currNode := mapleId2Node[mapleId]
            node2mapleJob[currNode].keysGenerate = keys



            for _, key := range keys {
                _, ok := keyMapleIdMap[key]
                if ok {
                    keyMapleIdMap[key] = append(keyMapleIdMap[key], mapleId)
                } else {
                    keyMapleIdMap[key] = []int{mapleId}
                }
            }
            fmt.Printf("Keys for %d maple id: %v, len of keys = %d\n", mapleId, keys, len(keys))
            
        }
        fmt.Printf("%v\n", keyMapleIdMap)
    }
    
    // assign key processing to worker nodes
    // do range or hash partitioning
    nodeIdx := 0
    
    // testguard = make(chan struct{}, 64) // limitng the number
    keyCount = len(keyMapleIdMap)
    for key := range keyMapleIdMap {
        _, ok := keyStatus[key]
        if ok && (keyStatus[key] == DONE || keyStatus[key] == ONGOING) {
            continue
        }
        currNode := workerNodes[nodeIdx]
        node2mapleJob[currNode].keysAggregate = append(node2mapleJob[currNode].keysAggregate, key)

        testguard <- struct{}{} // would block if guard channel is already filled
        go func(key string, respNode int, mapleIds []int) {
             ProcessKey(key, respNode, mapleIds)
            <-testguard
        }(key, currNode, keyMapleIdMap[key])
    
        nodeIdx = (nodeIdx + 1) % len(workerNodes)
    }

    select {
        case msg := <-connguard:
            fmt.Println("End connguard message %v \n", msg)
        default:
            fmt.Println("No message received\n")
    }
}

func ProcessKey(key string, respNode int, mapleIds []int) {
    keyStatus[key] = ONGOING
    // newguard <- struct{}{}
    fmt.Printf("Inside process key: key %s, respNode %d, maple ids that have this key: %v\n", key, respNode, mapleIds)

    keysFilename := fmt.Sprintf("%s_node.info", key)
    
    newguard <- struct{}{}
    activeFileNum = activeFileNum+1
    fmt.Printf("The number of active Files %d \n",activeFileNum)
    keyfile, err := os.Create(keysFilename)
    if err != nil {
        fmt.Printf("Could not create %s file\n", keysFilename)
        panic(err)
    }
    for _, mapleId := range mapleIds {
        // each record is mapleId:nodeId
        line := fmt.Sprintf("%d:%d", mapleId, mapleId2Node[mapleId])
        keyfile.WriteString(line + "$$$$")
    } 
    keyfile.Close()
    activeFileNum = activeFileNum-1
    fmt.Printf("The number of active Files %d \n",activeFileNum)
    <-newguard
    timeout := time.Duration(20) * time.Second

    nodeIP := memberMap[respNode].ip
    conn, err := net.DialTimeout("tcp", nodeIP + ":" + strconv.Itoa(fileTransferPort), timeout)
    if err != nil {
        log.Printf("[ME %d] Unable to connect with the master ip=%s port=%d", myVid, masterIP, masterPort)
        return
    }

    message := fmt.Sprintf("keyaggr %s %s", key, sdfsInterPrefix)
    padded_message := fillString(message, messageLength)

    conn.Write([]byte(padded_message))
    simpleSendFile(conn, keysFilename)
    keyTimeStamp[key] = time.Now().Unix()
}


func KeyAggregation(key string, nodeInfoList []string) {
    // var wg sync.WaitGroup
    // wg.Add(len(nodeInfoList))
    ch := make(chan bool, len(nodeInfoList))

    dataFileList := []string{}
    for _, nodeInfo := range(nodeInfoList) {
         
        splitNodeInfo:= strings.Split(nodeInfo, ":")
        mapleId_str := splitNodeInfo[0]
        nodeId_str := splitNodeInfo[1]
        nodeId, _ := strconv.Atoi(nodeId_str)
        dataFilePath := fmt.Sprintf("%soutput_%s_%s.out", maple_dir, mapleId_str, key)
        dataFileList = append(dataFileList, dataFilePath)
        connguard <- struct{}{}
        go getDirFile(nodeId, dataFilePath, dataFilePath, ch)
    }
    
    for i := 0; i < len(nodeInfoList); i++ {
        success := <- ch
        if !success {
            select {
                case msg := <-connguard:
                    fmt.Println("End connguard message %v \n", msg)
                default:
                    fmt.Println("No message received\n")
            } 
            return
        }
    }

    fmt.Printf("Got all files for key %s aggregation\n", key)

    outFilename := fmt.Sprintf("%s_inter.info", key)
    outFilePath := fmt.Sprintf("%s%s", local_dir, outFilename)
    
    AppendFiles(dataFileList, outFilePath)



    timeout := 20 * time.Second

    conn, err := net.DialTimeout("tcp", masterIP + ":" + strconv.Itoa(masterPort), timeout)
    if err != nil {
        log.Printf("[ME %d] Unable to connect with the master ip=%s port=%d", myVid, masterIP, masterPort)
        select {
            case msg := <-connguard:
                fmt.Println("End connguard message %v \n", msg)
            default:
                fmt.Println("No message received\n")
        } 
        return
    }

    PutFileWrapper(outFilename, sdfsInterPrefix + "_" + key, conn)
    conn.Close()
    // send an ack to the master
    conn, err = net.DialTimeout("tcp", masterIP + ":" + strconv.Itoa(masterPort), timeout)
      if err != nil {
        log.Printf("[ME %d] Unable to connect with the master ip=%s port=%d", myVid, masterIP, masterPort)
        select {
            case msg := <-connguard:
                fmt.Println("End connguard message %v \n", msg)
            default:
                fmt.Println("No message received\n")
        } 
        return
    }
    message := fmt.Sprintf("keyack %s\n", key)
    fmt.Printf("Sending %s\n",message)
    log.Printf("Sending %s\n",message)
    fmt.Fprintf(conn, message)
    conn.Close()
    select {
        case msg := <-connguard:
            fmt.Println("End connguard message %v \n", msg)
        default:
            fmt.Println("No message received\n")
    } 

    fmt.Printf("Appended the file for %s key\n", key)
    
    // put the appended file into sdfs and notify master
}

func getDirFile(destNodeId int, destFilePath string, localFilePath string, ch chan<- bool) {
    // get file 
    if !memberMap[destNodeId].alive{
        ch <- false
        select {
            case msg := <-connguard:
                fmt.Println("End connguard message %v \n", msg)
            default:
                fmt.Println("No message received\n")
        }
        return

    }
    timeout := time.Duration(20) * time.Second

    ip := memberMap[destNodeId].ip
    port := fileTransferPort

    conn, err := net.DialTimeout("tcp", ip + ":" + strconv.Itoa(port), timeout) 
    if err != nil {
        log.Printf("[ME %d] Unable to dial a connection to %d (to get file %s)\n", myVid, destNodeId, destFilePath)
        ch <- false
        select {
            case msg := <-connguard:
                fmt.Println("End connguard message %v \n", msg)
            default:
                fmt.Println("No message received\n")
        }
        connguard <- struct{}{}
        go getDirFile(destNodeId,destFilePath,localFilePath,ch)
        return
    }
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
        ch <- false
        select {
            case msg := <-connguard:
                fmt.Println("End connguard message %v \n", msg)
            default:
                fmt.Println("No message received\n")
        }
        connguard <- struct{}{}
        go getDirFile(destNodeId,destFilePath,localFilePath,ch)
        return
    }
    fileSize, _ := strconv.ParseInt(strings.Trim(string(bufferFileSize), filler), 10, 64)

    log.Printf("[ME %d] Incoming file size %d", myVid, fileSize)
    // fmt.Printf("[ME %d] Incoming file size %d", myVid, fileSize)
    newguard <- struct{}{}
    activeFileNum = activeFileNum+1
    fmt.Printf("The number of active Files %d \n",activeFileNum)
    file, err := os.Create(localFilePath)
    if err != nil {
        log.Printf("[ME %d] Cannot create the local file %s", myVid, localFilePath) 
        ch <- false
        activeFileNum = activeFileNum-1
        fmt.Printf("The number of active Files %d \n",activeFileNum)
        select {
            case msg := <-newguard:
                fmt.Println("End newguard message %v \n", msg)
            default:
                fmt.Println("moveove message received\n")
        }
        select {
            case msg := <-connguard:
                fmt.Println("End connguard message %v \n", msg)
            default:
                fmt.Println("No message received\n")
        }
        connguard <- struct{}{}
        go getDirFile(destNodeId,destFilePath,localFilePath,ch)
        return
    }
    // defer file.Close()

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
    }
    ch <- success
    file.Close()
    activeFileNum = activeFileNum-1
    fmt.Printf("The number of active Files %d \n",activeFileNum)
    select {
        case msg := <-newguard:
            fmt.Println("End newguard message %v \n",msg)
        default:
            fmt.Println("moveove message received \n")
    }
    select {
        case msg := <-connguard:
            fmt.Println("End connguard message %v \n", msg)
        default:
            fmt.Println("No message received\n")
    }
    return    
}

func AppendFiles(inputFilePaths []string, outFilePath string) {
    newguard <- struct{}{}
    activeFileNum = activeFileNum+1
    fmt.Printf("The number of active Files %d \n",activeFileNum)
    outfile, err := os.Create(outFilePath)
    if err != nil {
        fmt.Printf("Could not open %s file for appending\n", outFilePath)
        panic(err)
    }

    for _, inputfile := range inputFilePaths {
        newguard <- struct{}{}
        activeFileNum = activeFileNum+1
        fmt.Printf("The number of active Files %d \n",activeFileNum)
        fIn, err := os.Open(inputfile)
        if err != nil {
            fmt.Printf("Could not open %s file for appending\n", inputfile)
            panic(err)
        } 

        _, err = io.Copy(outfile, fIn)
        if err != nil {
            fmt.Printf("Could not copy the file from %s to %s\n", inputfile, outFilePath)
        }
        fIn.Close()
        activeFileNum = activeFileNum-1
        fmt.Printf("The number of active Files %d \n",activeFileNum)
        <-newguard
    }

    outfile.Close()
    activeFileNum = activeFileNum-1
    fmt.Printf("The number of active Files %d \n",activeFileNum)
    
     <-newguard
    fmt.Printf("Merged all files %v\n", inputFilePaths)

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

func removeFromList(l []int, target int) {
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
    
}

func handleMapleFailure(subject int) {


    if mapleRunning {
        _, isNodeMaple := node2mapleJob[subject]
        if isNodeMaple {
            fmt.Printf("Rerunning the maple task for failed nodes %d \n",subject)
            // this node is running maple

            // [TODO] what is the system does not have enough nodes to satisfy this req, handle that
            replacement := getRandomNodes(append(workerNodes, 0), 1)[0]
            var jobnode mapleJob
            jobnode.assignedMapleIds = node2mapleJob[subject].assignedMapleIds
            jobnode.keysAggregate = node2mapleJob[subject].keysAggregate
            jobnode.keysGenerate = node2mapleJob[subject].keysGenerate
            node2mapleJob[replacement] = &jobnode

            removeFromList(workerNodes, subject)
            workerNodes = append(workerNodes, replacement)

            if !mapleBarrier {
                // re-run all the maple ids assigned to this node
                assignedMapleIds := node2mapleJob[subject].assignedMapleIds
                for _, mapleid := range(assignedMapleIds) {
                    mapleId2Node[mapleid] = replacement
                    mapleCompMap[mapleid] = false
                    connguard <- struct{}{}
                    go sendMapleInfo(replacement, mapleid, sdfsMapleExe, mapleFiles[mapleid])
                }
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
                        keyStatus[keyGen] = FAILED
                    }
                    for _, keyAggr := range node2mapleJob[subject].keysAggregate {
                        keyStatus[keyAggr] = FAILED
                    }
                    mapleBarrier = false
                    assignedMapleIds := node2mapleJob[subject].assignedMapleIds
                    for _, mapleid := range(assignedMapleIds) {
                        connguard <- struct{}{}
                        go sendMapleInfo(replacement, mapleid, sdfsMapleExe, mapleFiles[mapleid])
                    }
                } else{
                    // testguard := make(chan struct{}, 50)
                    for _, keyAggr := range node2mapleJob[subject].keysAggregate {
                        if keyStatus[keyAggr] != DONE {
                            keyStatus[keyAggr] = FAILED
                            testguard <- struct{}{} // would block if guard channel is already filled
                            go func(key string, respNode int, mapleIds []int) {
                                 ProcessKey(key, respNode, mapleIds)
                                <-testguard
                            }(keyAggr, replacement, keyMapleIdMap[keyAggr])
                            // go ProcessKey(keyAggr, replacement, keyMapleIdMap[keyAggr])
                        }  
                    } 
                }
            }
        }
    }
}

func deleteMap(m map[interface{}]interface{}) {
    for k := range m {
        delete(m, k)
    } 
}



