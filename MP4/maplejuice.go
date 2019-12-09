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
    "errors"
    // "math"
)

// type status int
const (
    FAILED int = 0
    ONGOING int = 1
    DONE int = 2
)

const BUFFERSIZE = 1024

type fileData struct {
    timestamp int64
    nodeIds []int
}

type nodeData struct {
    fileList map[string]int64
}

type conflictData struct{
    id int
    timestamp int64
}

type mapleJob struct {
    assignedMapleIds []int
    keysGenerate []string
    keysAggregate []string
}

type juiceJob struct {
    assignedJuiceIds []int
    keysGenerate []string
    keysAggregate []string
}

var local_dir = "local/"
var shared_dir = "shared/"
var temp_dir = "temp/"
var maple_dir = "maple/"
var juice_dir = "juice/"


var replicatePeriod = 30

var masterIP = "172.22.152.106"
var masterPort = 8085
var fileTransferPort = 8084
var masterNodeId = 0

var maxGoroutines = 128

var ongoingElection = false
var electiondone = make(chan bool, 1)

// sdfs related
var fileMap = make(map[string]*fileData)
var nodeMap = map[int]map[string]int64{}
var conflictMap = make(map[string]*conflictData)
var fileTimeMap = make(map[string]int64)


// maple related
// master
var mapleId2Node = make(map[int]int)
var juiceId2Node = make(map[int]int)

var mapleCompMap = make(map[int]bool)
var juiceCompMap = make(map[int]int)
var juiceTimeStamp = make(map[int]int64) 

var node2mapleJob = make(map[int]*mapleJob)
var node2juiceJob = make(map[int]*juiceJob)

var keyStatus = make(map[string]int)
var keyDone = make(map[string]bool)
var keyTimeStamp = make(map[string]int64) // For rerun a key
var workerNodes []int

var mapleBarrier = false

var mapleRunning = false
var juiceRunning = false

var mapleInitTime time.Time
var juiceInitTime time.Time

var sdfsMapleExe string
var sdfsJuiceExe string

var mapleFiles []string
var juiceFiles []string

var keyMapleIdMap = make(map[string][]int)
var keyJuiceIdMap = make(map[string][]int)
// var failRerunMap = make(map[int]bool)

var JuiceQueue []string
var mutex = &sync.Mutex{}
var fileMutex = &sync.Mutex{}
// juice related

// var juiceInitTime

var sdfsDestJuiceName string

var keyCount = 0
var juiceCount = 0
// master + others
var sdfsInterPrefix string
var sdfsJuiceInterPrefix string

// var newguard = make(chan struct{}, maxGoroutines)
// var connguard = make(chan struct{}, 256)
// var testguard = make(chan struct{}, 64)
var connTokensCount = 800
var connTokens = make(chan bool, connTokensCount)

var fileTokensCount = 3000
var fileTokens = make(chan bool, fileTokensCount)

var parallelCount = 100
var parallelToken = make(chan bool, parallelCount)

var juicePCount = 200
var juicePToken = make(chan bool, juicePCount)

var juiceTokenCount = 20
var juiceToken = make(chan bool,juiceTokenCount)

var activeFileNum = 0

var messageLength = 256
var filler = "^"

var mapleJuicePort = 8079
var ackPort = 8078
var getPort = 8077
var putPort = 8076


var activeConnCount = 0
var activeFileCount = 0
var activeParallelCount = 0


var keyBatchChans []chan bool

func listenMapleJuicePort() {
    ln, err := net.Listen("tcp", ":" + strconv.Itoa(mapleJuicePort))
    if err != nil {
        fmt.Println(err)
    }

    for {
        if myIP == masterIP {

            conn, err := ln.Accept()
            if err != nil{
                fmt.Println(err)
            }
            log.Printf("[ME %d] Accepted a new connection on the master port %d\n", myVid, masterPort)     

            conn_reader := bufio.NewReader(conn)
            message, _ := conn_reader.ReadString('\n')
            if len(message) > 0 {
                // remove the '\n' char at the end
                message = message[:len(message)-1]
                fmt.Printf("")
            }
            
            split_message := strings.Split(message, " ")
            message_type := split_message[0]

            switch message_type {
            case "keyJuice":
                if !juiceRunning{
                    break
                }
                juiceId,_ := strconv.Atoi(split_message[1])
                if juiceCompMap[juiceId] == DONE{
                    break
                }

                sender,_ := strconv.Atoi(split_message[2])
                juiceOutPut := split_message[3]
                checkOut:= strings.Trim(juiceOutPut," ")
                if len(checkOut)<2{
                    fmt.Printf("WRONGJUICE Output : %s , juiceID : %d\n",juiceOutPut,juiceId)
                    log.Printf("WRONGJUICE Output : %s , juiceID : %d\n",juiceOutPut,juiceId)
                    break
                }

                if juiceCompMap[juiceId] != DONE{
                    juiceCount = juiceCount -1
                    log.Printf("JUICE RECVD %d from %d OutPut : %s, remaining keys %d \n",juiceId,sender,juiceOutPut,juiceCount)
                    fmt.Printf("JUICE RECVD %d from %d OutPut : %s, remaining keys %d \n",juiceId,sender,juiceOutPut,juiceCount)
                }
                // juiceCompMap[juiceId] = DONE
                

                // Append in the file 
                jFileName := fmt.Sprintf("%s%s.juice",juice_dir,sdfsDestJuiceName)
                acquireFile()
                tempFile, err := os.OpenFile(jFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
                if err!= nil{
                    fmt.Printf("Unable to open file for Writing %s \n",juiceOutPut)
                    releaseFile()
                    break
                }
                outline := fmt.Sprintf("%s\n",juiceOutPut)
                tempFile.WriteString(outline)
                tempFile.Close()
                releaseFile()
                juiceCompMap[juiceId] = DONE



                fmt.Printf("%d key has been processed and the corresponding sdfs is added\n", juiceId)

                success := true
                for tempKey := range juiceCompMap {
                    if juiceCompMap[tempKey] != DONE {
                        // fmt.Printf("!!!!!!!!!!!!!!!!!!!!!!!!!!!!Key %s not done \n",tempKey)
                        success = false
                        break
                    }
                }

                if success {
                    fmt.Printf("Juice task completed successfully\n")
                    juiceRunning = false
                    // clear all the data structures, maple, delete misc temp files
                    // for tempKey := range(keyStatus) {
                    //     fmt.Printf("%s ", tempKey)
                    // }
                    // fmt.Printf("\n")
                    //Clean the data structure

                    for k := range juiceId2Node {
                        delete(juiceId2Node, k)
                    }
                    for k := range juiceCompMap{
                        delete(juiceCompMap, k)
                    }
                    for k := range node2juiceJob {
                        delete(node2juiceJob, k)
                    }

                    workerNodes = nil

                    // mapleBarrier = false

                    // for k := range(keyJuiceIdMap){
                    //     delete(keyJuiceIdMap, k)
                    // }

                    // for k := range(keyStatus){
                    //     delete(keyStatus, k)
                    // }


                    elapsed := time.Since(juiceInitTime)
                    fmt.Printf("Time taken for Juice to finish %s\n", elapsed)
                    log.Printf("Time taken for Juice to finish %s\n", elapsed)

                    log.Printf("*********************************************\n")
                    log.Printf("***********   JUICE DONE ********************\n")
                    log.Printf("*********************************************\n")

                    fmt.Printf("*********************************************\n")
                    fmt.Printf("***********   JUICE DONE ********************\n")
                    fmt.Printf("*********************************************\n")
                    // os.Exit(1)
                    
                }

            case "keyack":

                if !mapleRunning{
                    break
                }

                key := split_message[1]
                if keyDone[key]{
                    break
                }
                sender,_ := strconv.Atoi(split_message[2])
                if !keyDone[key]{
                    keyCount = keyCount -1
                    log.Printf("keyack RECVD %s from %d , remaining keys %d \n",key,sender,keyCount)
                    fmt.Printf("keyack RECVD %s from %d, remaining keys %d \n",key,sender,keyCount)
                }
                keyStatus[key] = DONE
                keyDone[key] = true
                

                fmt.Printf("%s key has been processed and the corresponding sdfs is added\n", key)

                success := true
                for tempKey := range keyDone {
                    if !keyDone[tempKey] {
                        // fmt.Printf("!!!!!!!!!!!!!!!!!!!!!!!!!!!!Key %s not done \n",tempKey)
                        success = false
                        break
                    }
                }

                if success {
                    fmt.Printf("Maple task completed successfully\n")
                    mapleRunning = false
                    // clear all the data structures, maple, delete misc temp files
                    // for tempKey := range(keyStatus) {
                    //     fmt.Printf("%s ", tempKey)
                    // }
                    // fmt.Printf("\n")
                    //Clean the data structure

                    for k := range mapleId2Node {
                        delete(mapleId2Node, k)
                    }
                    for k := range mapleCompMap{
                        delete(mapleCompMap, k)
                    }
                    for k := range node2mapleJob {
                        delete(node2mapleJob, k)
                    }

                    workerNodes = nil

                    mapleBarrier = false

                    for k := range(keyMapleIdMap){
                        delete(keyMapleIdMap, k)
                    }

                    for k := range(keyStatus){
                        delete(keyStatus, k)
                    }


                    elapsed := time.Since(mapleInitTime)
                    fmt.Printf("Time taken for maple to finish %s\n", elapsed)
                    log.Printf("Time taken for maple to finish %s\n", elapsed)




                    log.Printf("*********************************************\n")
                    log.Printf("***********   MAPLE DONE ********************\n")
                    log.Printf("*********************************************\n")
                    
                    fmt.Printf("*********************************************\n")
                    fmt.Printf("***********   MAPLE DONE ********************\n")
                    fmt.Printf("*********************************************\n")

                    // os.Exit(1)
                    
                }

            }
        }
    }
}


func listenAckPort() {
    ln, err := net.Listen("tcp", ":" + strconv.Itoa(ackPort))
    if err != nil {
        fmt.Println(err)
    }

    for {
        if myIP == masterIP {

            conn, err := ln.Accept()
            if err != nil{
                fmt.Println(err)
            }
            log.Printf("[ME %d] Accepted a new connection on the master port %d\n", myVid, ackPort)     

            conn_reader := bufio.NewReader(conn)
            message, _ := conn_reader.ReadString('\n')
            if len(message) > 0 {
                // remove the '\n' char at the end
                message = message[:len(message)-1]
                fmt.Printf("")
            }
            
            split_message := strings.Split(message, " ")
            message_type := split_message[0]

            switch message_type {
                ////
                case "ack": 
                    /*
                        "ack action srcNode destNode(s) sdfsFilename"

                        if action is put, it means quorum has been reached 
                        for file sdfsFilename and with destNodes. 
                        now the master must send a confirmation to destNodes 
                        to "movefile" from temp dir to shared dir. 
                        Also update fileMap and nodeMap. 

                        Similarly for replicate, the only difference is --  
                        for put quorum is reached (for 4 nodes), whereas replicate
                        means the file has been replicated to destNode (only one node).
                    */

                    action := split_message[1]
                    srcNode, err := strconv.Atoi(split_message[2])
                    if err != nil {
                        log.Printf("[ME %d] Cannot convert %s to an int node id\n", myVid, split_message[2])
                        break
                    }
                    destNodes_str := split_message[3]
                    sdfsFilename := split_message[4]

                    if action == "put" {
                        
                        /*
                            respond only if the quorum is for the lastest put on sdfsFilename
                            if write-write conflict happens and the user replies yes;
                            then the earlier quorum ack can be ignored 
                            => respond only if the quorum ack corresponds to the last put on sdfsFilename
                        */ 

                        if conflictMap[sdfsFilename].id == srcNode {

                            destNodes := removeDuplicates(string2List(destNodes_str))
                            for _, node := range(destNodes) {
                                go sendConfirmation(node, sdfsFilename, srcNode)
                            }

                            updateTimestamp := time.Now().Unix()
                            var newfiledata fileData 
                            newfiledata.timestamp = updateTimestamp
                            if len(destNodes) > 4{
                                destNodes = destNodes[:4]
                            }
                            newfiledata.nodeIds = destNodes
                            fileMap[sdfsFilename] = &newfiledata

                            for _, node := range(destNodes) {
                                _, ok := nodeMap[node]
                                if ok {
                                    nodeMap[node][sdfsFilename] = updateTimestamp
                                } else {
                                    nodeMap[node] = make(map[string]int64)
                                    nodeMap[node][sdfsFilename] = updateTimestamp
                                }
                            }
                        } else {
                            // ignore
                        }
                    } else if action == "replicate" {
                        destNodes := removeDuplicates(string2List(destNodes_str))
                        destNode := destNodes[0]

                        go sendConfirmation(destNode, sdfsFilename, srcNode)
                        
                        updateTimestamp := time.Now().Unix()
                        if len(fileMap[sdfsFilename].nodeIds) < 4 {
                            toAdd := true
                            for _,elem := range fileMap[sdfsFilename].nodeIds{
                                if elem == destNode{
                                    toAdd = false
                                    break
                                }
                            }
                            if !toAdd{
                                break
                            }
                            fileMap[sdfsFilename].nodeIds = append(fileMap[sdfsFilename].nodeIds, destNode)
                        }else{
                            break
                        }

                        _, ok := nodeMap[destNode]
                        if ok {
                            nodeMap[destNode][sdfsFilename] = updateTimestamp
                        } else {
                            nodeMap[destNode] = make(map[string]int64)
                            nodeMap[destNode][sdfsFilename] = updateTimestamp
                        } 

                        fmt.Printf("replicate %s complete, time now = %d\n", sdfsFilename, time.Now().UnixNano())                   
                    }


            }
        }
    }
}


func listenGetPort() {
    ln, err := net.Listen("tcp", ":" + strconv.Itoa(getPort))
    if err != nil {
        fmt.Println(err)
    }

    // fmt.Printf("Started listening on Get port %d\n", getPort)
    log.Printf("Started listening on GETPORT port %d\n", getPort)
    

    for {
        if myIP == masterIP {

            conn, err := ln.Accept()
            if err != nil{
                fmt.Println(err)
            }
            log.Printf("[ME %d] Accepted a new connection on the GETPORT port %d\n", myVid, getPort)     

            conn_reader := bufio.NewReader(conn)
            message, _ := conn_reader.ReadString('\n')
            if len(message) > 0 {
                // remove the '\n' char at the end
                message = message[:len(message)-1]
            }
            
            split_message := strings.Split(message, " ")
            message_type := split_message[0]

            switch message_type {
                ////
                case "get":
                /*
                    "get sdfsFilename"

                    returns a list of nodes that have the sdfsFilename.
                    if the file does not exist (in the shared file system)
                    it replies with an empty list.
                */

                    fmt.Printf("Received a get request %s\n", message)
                    log.Printf("Received a GETPORT request %s\n",message)
                    sdfsFilename := split_message[1]
                    _, ok := fileMap[sdfsFilename]
                    var nodes_str = ""

                    if ok {
                        fileMap[sdfsFilename].nodeIds = removeDuplicates(fileMap[sdfsFilename].nodeIds)
                        for _, node := range(fileMap[sdfsFilename].nodeIds) {
                            nodes_str = nodes_str + strconv.Itoa(node) + ","
                        }
                        if len(nodes_str) > 0 {
                            nodes_str = nodes_str[:len(nodes_str)-1]
                        }
                    }
                    reply := fmt.Sprintf("getreply %s %s\n", sdfsFilename, nodes_str)
                    log.Printf("GETPORT Reply for message :%s is %s \n",message,reply)
                    fmt.Fprintf(conn, reply)
                    fmt.Printf("Sent a reply %s\n", reply)

            }
        }
    }
}


func listenPutPort() {
    ln, err := net.Listen("tcp", ":" + strconv.Itoa(putPort))
    if err != nil {
        fmt.Println(err)
    }

    // fmt.Printf("Started listening on Get port %d\n", getPort)
    log.Printf("Started listening on PUTPORT port %d\n", getPort)

    for {
        if myIP == masterIP {

            conn, err := ln.Accept()
            if err != nil{
                fmt.Println(err)
            }
            log.Printf("[ME %d] Accepted a new connection on the PUTPORT port %d\n", myVid, getPort)     

            conn_reader := bufio.NewReader(conn)
            message, _ := conn_reader.ReadString('\n')
            if len(message) > 0 {
                // remove the '\n' char at the end
                message = message[:len(message)-1]
                // fmt.Printf("")
            }
            
            split_message := strings.Split(message, " ")
            message_type := split_message[0]

            switch message_type {
                ////
                case "put":
                /*
                    "put sdfsFilename sender"

                    `sender` node wants to put (insert/update) `sdfsFilename` file
                    master must resolve write-write conflicts if any and reply with 
                    a list of nodes (selected randomly for insert or from fileMap for update).
                    Uses conflictMap to store last executed put for sdfsFilename.
                */

                fmt.Printf("Received a put request %s\n", message)

                sdfsFilename := split_message[1]
                sender, _ := strconv.Atoi(split_message[2])

                var newConflict conflictData
                newConflict.id = sender
                newConflict.timestamp = time.Now().Unix()
                conflictMap[sdfsFilename] = &newConflict

                // _, ok := conflictMap[sdfsFilename]
                // if ok {
                //     fmt.Printf("file already in conflictMap\n")
                //     var newConflict conflictData
                //     newConflict.id = sender
                //     newConflict.timestamp = time.Now().Unix()
                //     conflictMap[sdfsFilename] = &newConflict
                // } else{
                //     fmt.Printf("file not in conflictMap\n")
                //     // sdfsFilename not in conflictMap 
                //     var newConflict conflictData
                //     newConflict.id = sender
                //     newConflict.timestamp = time.Now().Unix()
                //     conflictMap[sdfsFilename] = &newConflict
                // }

                /*
                    Congratulations, your put request has cleared all hurdles
                    processing the put request now
                */

                _, ok := fileMap[sdfsFilename]
                if ok {
                    fmt.Printf("file already exists, sending old nodes\n")
                    var nodes_str = ""
                    for _, node := range(fileMap[sdfsFilename].nodeIds) {
                        nodes_str = nodes_str + strconv.Itoa(node) + ","
                    }
                    if len(nodes_str) > 0{
                        nodes_str = nodes_str[:len(nodes_str)-1]
                    }

                    reply := fmt.Sprintf("putreply %s %s\n", sdfsFilename, nodes_str)
                    fmt.Fprintf(conn, reply)

                } else {
                    fmt.Printf("file new, send new nodes\n");
                    var excludelist = []int{sender}
                    // get three random nodes other than the sender itself
                    nodes := getRandomNodes(excludelist, 3)
                    nodes = append(nodes, sender)

                    var nodes_str = ""
                    for _, node := range(nodes) {
                        nodes_str = nodes_str + strconv.Itoa(node) + ","
                    }
                    if len(nodes_str) > 0 {
                        nodes_str = nodes_str[:len(nodes_str)-1]
                    }

                    reply := fmt.Sprintf("putreply %s %s\n", sdfsFilename, nodes_str)
                    fmt.Printf("Sending this reply for the put request %s\n", reply)
                    fmt.Fprintf(conn, reply)
                }

            }
        }
    }
}



func fillString(givenString string, toLength int) string {
    // pads `givenString` with ':' to make it of `toLength` size
    for {
        lengthString := len(givenString)
        if lengthString < toLength {
            givenString = givenString + filler
            continue
        }
        break
    }
    return givenString
}

// func releaseConn(){
//     select {
//         case msg := <-connguard:
//             fmt.Println("End connguard message %v \n", msg)
//         default:
//             fmt.Println("Why you be trying to pop empty connguard?\n")
//     }

// }

// func releaseGuard(){
//     select {
//         case msg := <-newguard:
//             fmt.Println("End connguard message %v \n", msg)
//         default:
//             fmt.Println("Why you be trying to pop empty newguard?\n")
//     }

// }

func acquireConn() {
    // fmt.Printf("Acquiring conn token, curr count: %d\n", activeConnCount)
    <- connTokens
    activeConnCount = activeConnCount + 1
    // fmt.Printf("Acquired conn token, curr count %d \n", activeConnCount)
}

func releaseConn() {
    connTokens <- true
    activeConnCount = activeConnCount - 1
    // fmt.Printf("Released conn token, curr count %d \n", activeConnCount)

    // select {
    //     case msg := <-connTokens:
    //         fmt.Printf("released conn %v\n", msg)
    //     default:
    //         fmt.Println("Why you be trying to pop empty connTokens?\n")
    // }
}

func acquireJuice() {
    // fmt.Printf("Acquiring conn token, curr count: %d\n", activeConnCount)
    <- juicePToken
    // activeConnCount = activeConnCount + 1
    // fmt.Printf("Acquired conn token, curr count %d \n", activeConnCount)
}

func releaseJuice() {
    juicePToken <- true
    // activeConnCount = activeConnCount - 1
    // fmt.Printf("Released conn token, curr count %d \n", activeConnCount)

    // select {
    //     case msg := <-connTokens:
    //         fmt.Printf("released conn %v\n", msg)
    //     default:
    //         fmt.Println("Why you be trying to pop empty connTokens?\n")
    // }
}

func acquireFile() {
    // fmt.Printf("Acquiring file token, curr count: %d\n", activeFileCount)
    <- fileTokens
    activeFileCount = activeFileCount + 1
    // fmt.Printf("Acquired file token, curr count %d \n", activeFileCount)

    // fileTokens <- true
}

func releaseFile() {
    fileTokens <- true
    activeFileCount = activeFileCount - 1
    // fmt.Printf("Released file token, curr count %d \n", activeFileCount)

    // select {
    //     case msg := <-fileTokens:
    //         fmt.Printf("released file %v\n", msg)
    //     default:
    //         fmt.Println("Why you be trying to pop empty fileTokens?\n")
    // }
}

func acquireParallel() {
    // fmt.Printf("Acquiring parallel token, curr count: %d\n", activeParallelCount)
    <- parallelToken
    activeParallelCount = activeParallelCount + 1
    // fmt.Printf("Acquired parallel token, curr count %d \n", activeParallelCount)

    // activeFileCount = activeFileCount+1
    // fmt.Printf("Active File Count %d \n",activeFileCount)

    // fileTokens <- true
}

func releaseParallel() {
    parallelToken <- true
    activeParallelCount = activeParallelCount - 1
    // fmt.Printf("Released parallel token, curr count %d \n", activeParallelCount)

    // activeFileCount = activeFileCount-1
    // fmt.Printf("Active File Count %d \n",activeFileCount)

    // select {
    //     case msg := <-fileTokens:
    //         fmt.Printf("released file %v\n", msg)
    //     default:
    //         fmt.Println("Why you be trying to pop empty fileTokens?\n")
    // }
}




func getmyIP() (string) {
    // helper func to get my IP
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


func list2String(list []int) (string) {
    // helper func to convert list of int(s) to a comma separated string (no spaces)
    var list_str = ""
    for _, element := range(list) {
        list_str = list_str + strconv.Itoa(element) + ","
    }
    if len(list_str) > 0 {
        list_str = list_str[:len(list_str)-1]
    }
    return list_str
}

func listenFileTransferPort() {
    /*
        This func is run as a go routine that listens on the `fileTransferPort` 
        and transfers file over the network to complete `get`, `put`, `replicate` requests.
        Also handles `delete` file.  
    */

    ln, err := net.Listen("tcp", ":" + strconv.Itoa(fileTransferPort))
    if err != nil {
        log.Printf("[ME %d] Cannot listen on file transfer port %d\n", myVid, fileTransferPort)
    }

    log.Printf("[ME %d] Started listening on file transfer port %d\n", myVid, fileTransferPort)

    for {
        // acquireConn()
        conn, _ := ln.Accept()
        log.Printf("[ME %d] Accepted a new connection from %s\n", myVid, conn.RemoteAddr().(*net.TCPAddr).IP.String())     
        bufferMessage := make([]byte, messageLength)
        
        conn.Read(bufferMessage)
        message := strings.Trim(string(bufferMessage), filler)
        
        log.Printf("[ME %d] Received message %s on the file transfer port %d\n", myVid, message, fileTransferPort)

        split_message := strings.Split(message, " ")
        message_type := split_message[0]

        switch message_type {

        case "keyaggr":
            key := split_message[1]
            sdfsInterPrefix = split_message[2]
            nodeInfofilePath := split_message[3]
            sender,_ := strconv.Atoi(split_message[4])
            // fmt.Printf("Inside keyaggr: I am going to receive the nodes ids for key %s %s\n", key, sdfsInterPrefix)

            // nodeInfoFile := simpleRecvFile(conn) // Could be an issue Use getDirFile
            // if len(nodeInfoFile) == 0 {
            //     break
            // }
            ch := make(chan bool,1)

            getDirFile(sender,nodeInfofilePath,nodeInfofilePath,ch,3)
            // for{
            //     success:= <- ch
            //     if !success{
            //         fmt.Printf("Failed to getDir the file for KeyAggr %s\n",key)
            //         log.Printf("Failed to getDir the file for KeyAggr %s\n",key)
            //         getDirFile(sender,nodeInfofilePath,nodeInfofilePath,ch,1)
            //     }else{
            //         fmt.Printf("KeyAggr getDir file  received for key : %s\n", key)
            //         log.Printf("KeyAggr getDir file  received for key : %s\n", key)
            //         break
            //     }
            // }
            fmt.Printf("Received the key file for processing %s\n", key)
            // newguard <- struct{}{}
            // activeFileNum = activeFileNum+1
            // fmt.Printf("The number of active Files %d \n",activeFileNum)

            acquireFile()
            fileAgg, err := os.Open(nodeInfofilePath)   
            if err != nil {
                // log.Panicf("failed reading file: %s", err)
                // activeFileNum = activeFileNum-1
                // fmt.Printf("The number of active Files %d \n",activeFileNum)
                log.Printf("[KEYAGGR] Could not open the file %s\n",nodeInfofilePath)
                fmt.Printf("[KEYAGGR] Could not open the file %s\n",nodeInfofilePath)

                releaseFile()
                break
                // panic(err)
            }

            contentBytes, err := ioutil.ReadAll(fileAgg)
            if err != nil {
                fmt.Printf("Could not read file %s corresponding to %s key\n", nodeInfofilePath, key)
                log.Printf("Could not read file %s corresponding to %s key\n", nodeInfofilePath, key)

                // panic(err)
            }

            fileAgg.Close()
            releaseFile()

            // activeFileNum = activeFileNum-1
            // fmt.Printf("The number of active Files %d \n",activeFileNum)
            // releaseGuard()

            content := string(contentBytes)
            nodeInfoList := strings.Split(content, "$$$$")
            nodeInfoList = nodeInfoList[:len(nodeInfoList)-1]
            
            // connguard <- struct{}{}
            // acquireParallel()
            // go func (Lkey string, nList []string) {
            //     KeyAggregation(Lkey,nList)
            //     releaseParallel()
                
            // }(key,nodeInfoList)
            go KeyAggregation(key, nodeInfoList)
            // <-newguard


        case "keyfile":

            action := split_message[1]
            if action == "maple" {
                sender, err := strconv.Atoi(split_message[2])
                if err != nil {
                    log.Printf("Could not convert sender %s into an int\n", split_message[2])
                    fmt.Printf("Could not convert sender %s into an int\n", split_message[2])

                    panic(err)
                }

                mapleId, err := strconv.Atoi(split_message[3])
                if err != nil {
                    log.Printf("Could not convert maple id %s into an int\n", split_message[3])
                    fmt.Printf("Could not convert maple id %s into an int\n", split_message[3])

                    panic(err)
                }

                keysFilename := simpleRecvFile(conn)
                fmt.Printf("%d node finished maple id %d execution\n", sender, mapleId)
                log.Printf("%d node finished maple id %d execution\n", sender, mapleId)

                mapleCompMap[mapleId] = true
                fmt.Printf("Received %s file corresponding to %d mapleId\n", keysFilename, mapleId)

                success := true
                for mapleId := range(mapleCompMap) {
                    if !mapleCompMap[mapleId] {
                        success = false
                        break
                    }
                }

                if success {
                    fmt.Printf("all maple executions finished\n")
                    log.Printf("[MAPLE] All maple executions finished\n")
                    mapleBarrier = true
                    // connguard <- struct{}{}
                    go AssembleKeyFiles()
                }
                
            }

        case "runmaple":
            /*
            runmaple mapleId sdfsMapleExe inputFile
            */
                        
            mapleId, _ := strconv.Atoi(split_message[1])

            sdfsMapleExe := split_message[2]
            localMapleExe := sdfsMapleExe
            if !getFileWrapper(sdfsMapleExe, localMapleExe){
                getFileWrapper(sdfsMapleExe, localMapleExe)
            }
            fmt.Printf("Got the maple exe, check my local folder\n");

            inputFile := split_message[3]
            localInputFile := inputFile
            if !getFileWrapper(inputFile, localInputFile){
                getFileWrapper(inputFile, localInputFile)
            }
            fmt.Printf("I got the file %s %s\n", inputFile, localInputFile)

            exeFile := fmt.Sprintf("local/%s", localMapleExe)
            inputFilePath := fmt.Sprintf("local/%s", localInputFile)

            outputFilePath := fmt.Sprintf("%soutput_%d.tempout", maple_dir, mapleId)
            ExecuteCommand(exeFile, inputFilePath, outputFilePath, mapleId)

            

        case "runjuice":
            /*
            runmaple mapleId sdfsMapleExe inputFile
            */
                        
                juiceId, _ := strconv.Atoi(split_message[1])

                sdfsJuiceExe = split_message[2]
                localJuiceExe := sdfsJuiceExe
                outputFilePath := fmt.Sprintf("%sJuice_%d.out", juice_dir, myVid)
                // check 
                if !juiceRunning {
                    juiceRunning = true
                    acquireFile()
                    outfile, err := os.Create(outputFilePath)
                    if err != nil{
                        fmt.Printf("Unable to create %s\n",outputFilePath)
                        log.Printf("Unable to create %s\n",outputFilePath)
                        releaseFile()
                        break
                    }
                    outfile.Close()
                    releaseFile()

                    for i:=0 ; i<5; i++{
                        if getFileWrapper(sdfsJuiceExe, localJuiceExe){
                            break
                        }
                    }

                    // if !getFileWrapper(sdfsJuiceExe, localJuiceExe){
                    //     getFileWrapper(sdfsJuiceExe, localJuiceExe)
                    // }

                    go JuiceProcess()

                }


                fmt.Printf("Got the Juice exe, check my local folder\n");

                inputFile := split_message[3]
                sdfsInputFile := inputFile
                // if !getFileWrapper(inputFile, localInputFile){
                //     getFileWrapper(inputFile, localInputFile)
                // }
                // fmt.Printf("I got the file %s %s\n", inputFile, localInputFile)

                exeFile := fmt.Sprintf("local/%s", localJuiceExe)
                // inputFilePath := fmt.Sprintf("local/%s", localInputFile)

                // outputFilePath := fmt.Sprintf("%soutput_%d.tempout", juice_dir, juiceId)

                juiceInp := fmt.Sprintf("%s %s %s %d",exeFile,sdfsInputFile,outputFilePath,juiceId)

                mutex.Lock()
                JuiceQueue = append(JuiceQueue,juiceInp)
                // fmt.Printf("The Juice JuiceQueue is %v \n",JuiceQueue)
                mutex.Unlock()
                go func() {
                    juiceToken <- true
                }()


            case "movefile":
                /*
                    "movefile sdfsFilename sender"

                    when quorum (of 4 nodes) is reached master sends this message  
                    to move file from temp directory to the shared directory. 
                    Add `sdfsFilename` to local `fileTimeMap`.
                */ 
                sdfsFilename := split_message[1]
                sender := split_message[2]

                tempFilePath := temp_dir + sdfsFilename + "." + sender
                sharedFilePath := shared_dir + sdfsFilename

                // newguard <- struct{}{}
                // activeFileNum = activeFileNum+1
                // fmt.Printf("The number of active Files %d \n",activeFileNum)

                acquireFile()
                err := os.Rename(tempFilePath, sharedFilePath)
                releaseFile()
                // activeFileNum = activeFileNum-1
                // fmt.Printf("The number of active Files %d \n",activeFileNum)
                // releaseGuard()
                if err != nil {
                    log.Printf("[ME %d] Could not move file %s to %s\n", myVid, tempFilePath, sharedFilePath)
                    break
                }

                fileTimeMap[sdfsFilename] = time.Now().Unix()

                log.Printf("[ME %d] Successfully moved file from %s to %s\n", myVid, tempFilePath, sharedFilePath)
                fmt.Printf("[ME %d] Successfully moved file from %s to %s\n", myVid, tempFilePath, sharedFilePath)

            case "getfile2":
                /* 
                    "getfile2 destFilePath"

                    sends `destFilePath` over the `conn`
                */

                filePath := split_message[1]

                _, err := os.Stat(filePath)
                if os.IsNotExist(err) {
                    // fmt.Printf("Got a get for %s, but the file does not exist\n", sdfsFilename)
                    log.Printf("[ME %d] Got a get for %s, but the file does not exist\n", myVid, filePath)
                    break
                }

                // newguard <- struct{}{}
                // activeFileNum = activeFileNum+1
                // fmt.Printf("The number of active Files %d \n",activeFileNum)

                acquireFile()
                f1_race, err := os.Open(filePath)
                if err != nil {
                    log.Printf("[ME %d] file open error: %s\n", err)
                    log.Printf("[ME %d] Can't open file %s\n", myVid, filePath)
                    releaseFile()
                    break
                }

                fileInfo, err := f1_race.Stat()
                if err != nil {
                    log.Printf("[ME %d] Can't access file stats for %s\n", myVid, filePath)
                    f1_race.Close()
                    releaseFile()
                    break
                }

                fileSize := fillString(strconv.FormatInt(fileInfo.Size(), 10), 10)
                log.Printf("[ME %d] Outgoing filesize %s", myVid, fileSize)
                conn.Write([]byte(fileSize))

                sendBuffer := make([]byte, BUFFERSIZE)

                success := true
                for {
                    _, err = f1_race.Read(sendBuffer)
                    if err != nil {
                        if err == io.EOF {
                            break
                        } else {
                            success = false
                            log.Printf("[ME %d] Error while reading file %s\n", myVid, filePath)
                            break
                        }
                    }
                    
                    _, err := conn.Write(sendBuffer)
                    if err != nil {
                        success = false
                        log.Printf("[ME %d] Error while sending file %s\n", myVid, filePath)
                        break
                    }
                }
                if success {
                    log.Printf("[ME %d] Successfully sent the complete file %s\n", myVid, filePath)
                } else {
                    log.Printf("[ME %d] Could not send the complete file %s\n", myVid, filePath)
                }

                f1_race.Close()
                releaseFile()
                // activeFileNum = activeFileNum-1
                // fmt.Printf("The number of active Files %d \n",activeFileNum)
                // releaseGuard()   

            case "getfile":
                /* 
                    "getfile sdfsFilename"

                    sends `sdfsFilename` over the `conn`
                */

                sdfsFilename := split_message[1]
                filePath := fmt.Sprintf("%s%s", shared_dir, sdfsFilename)

                _, err := os.Stat(filePath)

                if os.IsNotExist(err) {
                    // fmt.Printf("Got a get for %s, but the file does not exist\n", sdfsFilename)
                    log.Printf("[ME %d] Got a get for %s, but the file does not exist\n", myVid, sdfsFilename)
                    break
                }

                // newguard <- struct{}{}
                // activeFileNum = activeFileNum+1
                // fmt.Printf("The number of active Files %d \n",activeFileNum)

                acquireFile()
                f1_race, err := os.Open(filePath)
                if err != nil {
                    log.Printf("[ME %d] file open error: %s\n", err)
                    log.Printf("[ME %d] Can't open file %s\n", myVid, shared_dir + sdfsFilename)
                    releaseFile()
                    break
                }

                fileInfo, err := f1_race.Stat()
                if err != nil {
                    log.Printf("[ME %d] Can't access file stats for %s\n", myVid, sdfsFilename)
                    f1_race.Close()
                    releaseFile()
                    break
                }

                fileSize := fillString(strconv.FormatInt(fileInfo.Size(), 10), 10)
                log.Printf("[ME %d] Outgoing filesize %s", myVid, fileSize)
                conn.Write([]byte(fileSize))

                sendBuffer := make([]byte, BUFFERSIZE)

                success := true
                for {
                    _, err = f1_race.Read(sendBuffer)
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
                        break
                    }
                }
                if success {
                    log.Printf("[ME %d] Successfully sent the complete file %s\n", myVid, sdfsFilename)
                } else {
                    log.Printf("[ME %d] Could not send the complete file %s\n", myVid, sdfsFilename)
                }

                f1_race.Close()
                releaseFile()

                // activeFileNum = activeFileNum-1
                // fmt.Printf("The number of active Files %d \n",activeFileNum)
                // releaseGuard()


            case "putfile":
                /*
                    "putfile sdfsFilename sender"

                    write the file contents (coming over the conn) to your temp directory (for time being)
                    and send an ack to the sender (write "done" on conn)
                */

                sdfsFilename := split_message[1]
                sender := split_message[2]

                bufferFileSize := make([]byte, 10)
                _,err :=conn.Read(bufferFileSize)
                if err != nil{
                    fmt.Printf("Error in reading file Size for fileName %s \n",sdfsFilename)
                    break
                }
                fileSize, _ := strconv.ParseInt(strings.Trim(string(bufferFileSize), filler), 10, 64)

                log.Printf("[ME %d] Incoming filesize %d\n", myVid, fileSize)
                fmt.Printf("[ME %d] Incoming filesize %d\n", myVid, fileSize)
                log.Printf("[ME %d] Incoming fileName %s\n", myVid, sdfsFilename)
                fmt.Printf("[ME %d] Incoming fileName %s\n", myVid, sdfsFilename)



                // append sender to the tempFilePath to distinguish conflicting writes from multiple senders
                tempFilePath := temp_dir + sdfsFilename + "." + sender
                // newguard <- struct{}{}
                // activeFileNum = activeFileNum+1
                // fmt.Printf("The number of active Files %d \n",activeFileNum)

                acquireFile()
                f, err := os.Create(tempFilePath)
                if err != nil {
                    log.Printf("[ME %d] Cannot create file %s\n", myVid, sdfsFilename) 
                    releaseFile()
                    break
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
                        break
                    }
                    receivedBytes += BUFFERSIZE
                }

                // send ACK to sender
                if success {
                    fmt.Fprintf(conn, "done\n")
                    fmt.Printf("recvd file %s sender %s\n", sdfsFilename, sender)
                    log.Printf("[ME %d] Successfully received %s file from %s\n", myVid, sdfsFilename, sender)

                } else {
                    log.Printf("[ME %d] Couldn't receive the file %s from %d\n", myVid, sdfsFilename, sender)
                    fmt.Printf("[ME %d] Couldn't receive the file %s from %d\n", myVid, sdfsFilename, sender)
                }

                f.Close()
                releaseFile()

                // activeFileNum = activeFileNum-1
                // fmt.Printf("The number of active Files %d \n",activeFileNum)
                // releaseGuard()

            case "deletefile":
                /*
                    "deletefile sdfsFilename"

                    removes sdfsFilename from the shared directory and the local `fileTimeMap`
                */
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

                delete(fileTimeMap, sdfsFilename)

                log.Printf("[ME %d] Successfully deleted the file %s\n", myVid, sdfsFilename) 

            case "replicatefile":
                /*
                    "replicatefile sdfsFilename destNode"

                    replicate sdfsFilename on destNode
                */
                sdfsFilename := split_message[1]
                destNode, _ := strconv.Atoi(split_message[2])
                
                go replicateFile(destNode, sdfsFilename) 
        }
        conn.Close()
        // releaseConn()
    }
}


func listenMasterRequests() {
    /*
        This func is run as a go routine on the master node that listens on the `masterPort`
        master is the first point-of-contact to fulfill a get, put, delete request since it has all the meta-data.
        master uses fileMap to store the file -> list of nodes mapping and nodeMap to store the node -> files mapping.  
    */

    ln, err := net.Listen("tcp", ":" + strconv.Itoa(masterPort))
    if err != nil {
        fmt.Println(err)
    }

    for {
        if myIP == masterIP {

            // fmt.Printf("[MASTER] Waiting for a conn token to accept a new connection\n")
            // // acquireConn()
            // fmt.Printf("[MASTER] Received a conn token to accept a new connection\n")

            conn, err := ln.Accept()
            if err != nil{
                fmt.Println(err)
            }
            log.Printf("[ME %d] Accepted a new connection on the master port %d\n", myVid, masterPort)     

            conn_reader := bufio.NewReader(conn)
            message, _ := conn_reader.ReadString('\n')
            if len(message) > 0 {
                // remove the '\n' char at the end
                message = message[:len(message)-1]
                fmt.Printf("")
            }
            
            split_message := strings.Split(message, " ")
            message_type := split_message[0]

            switch message_type {
            case "file":
                /*
                    "file sender"
                    
                    After a new master is elected, each alive node sends its shared file names
                    to re-populate the meta-data (fileMap and nodeMap) at the new master. 
                */
                sender, _ := strconv.Atoi(split_message[1])
                for {
                    fileResp,err := conn_reader.ReadString('\n')
                    if err != nil{
                        log.Printf("[ME %d] Error in the file message from %d: %s\n", myVid, sender, err)
                    }
                    if len(fileResp) > 0 {
                        fileResp = fileResp[:len(fileResp)-1]
                    }
                    split_resp := strings.Split(fileResp, " ")
                    if split_resp[0] == "done" {
                        break
                    } else {
                        fileName := split_resp[2]
                        
                        _, ok := fileMap[fileName]
                        if ok {
                            fileMap[fileName].nodeIds = removeDuplicates(fileMap[fileName].nodeIds)
                            noAdd := true
                            for _,elem := range fileMap[fileName].nodeIds{
                                if sender == elem{
                                    noAdd = false
                                    break
                                }
                            }
                            if !noAdd{
                                continue
                            }
                            if len(fileMap[fileName].nodeIds) < 4 {
                                fileMap[fileName].nodeIds = append(fileMap[fileName].nodeIds,sender) 
                            }

                        } else {
                            var newfiledata fileData 
                            newfiledata.timestamp, _ = strconv.ParseInt(split_resp[3], 10, 64)
                            FileList := removeDuplicates(string2List(split_resp[1]))
                            if len(FileList) > 4{
                                FileList = FileList[:4]
                            }
                            newfiledata.nodeIds = FileList
                            fileMap[fileName] = &newfiledata
                        }
                        
                        _, ok = nodeMap[sender]
                        if ok {
                            nodeMap[sender][fileName] = fileMap[fileName].timestamp
                        } else {
                            nodeMap[sender] = make(map[string]int64)
                            nodeMap[sender][fileName] = fileMap[fileName].timestamp
                        }
                    }
                }

            case "put":
                /*
                    "put sdfsFilename sender"

                    `sender` node wants to put (insert/update) `sdfsFilename` file
                    master must resolve write-write conflicts if any and reply with 
                    a list of nodes (selected randomly for insert or from fileMap for update).
                    Uses conflictMap to store last executed put for sdfsFilename.
                */

                fmt.Printf("Received a put request %s\n", message)

                sdfsFilename := split_message[1]
                sender, _ := strconv.Atoi(split_message[2])

                var newConflict conflictData
                newConflict.id = sender
                newConflict.timestamp = time.Now().Unix()
                conflictMap[sdfsFilename] = &newConflict

                // _, ok := conflictMap[sdfsFilename]
                // if ok {
                //     fmt.Printf("file already in conflictMap\n")
                //     var newConflict conflictData
                //     newConflict.id = sender
                //     newConflict.timestamp = time.Now().Unix()
                //     conflictMap[sdfsFilename] = &newConflict
                // } else{
                //     fmt.Printf("file not in conflictMap\n")
                //     // sdfsFilename not in conflictMap 
                //     var newConflict conflictData
                //     newConflict.id = sender
                //     newConflict.timestamp = time.Now().Unix()
                //     conflictMap[sdfsFilename] = &newConflict
                // }

                /*
                    Congratulations, your put request has cleared all hurdles
                    processing the put request now
                */

                _, ok := fileMap[sdfsFilename]
                if ok {
                    fmt.Printf("file already exists, sending old nodes\n")
                    var nodes_str = ""
                    for _, node := range(fileMap[sdfsFilename].nodeIds) {
                        nodes_str = nodes_str + strconv.Itoa(node) + ","
                    }
                    if len(nodes_str) > 0{
                        nodes_str = nodes_str[:len(nodes_str)-1]
                    }

                    reply := fmt.Sprintf("putreply %s %s\n", sdfsFilename, nodes_str)
                    fmt.Fprintf(conn, reply)

                } else {
                    fmt.Printf("file new, send new nodes\n");
                    var excludelist = []int{sender}
                    // get three random nodes other than the sender itself
                    nodes := getRandomNodes(excludelist, 3)
                    nodes = append(nodes, sender)

                    var nodes_str = ""
                    for _, node := range(nodes) {
                        nodes_str = nodes_str + strconv.Itoa(node) + ","
                    }
                    if len(nodes_str) > 0 {
                        nodes_str = nodes_str[:len(nodes_str)-1]
                    }

                    reply := fmt.Sprintf("putreply %s %s\n", sdfsFilename, nodes_str)
                    fmt.Printf("Sending this reply for the put request %s\n", reply)
                    fmt.Fprintf(conn, reply)
                }

            case "get":
                /*
                    "get sdfsFilename"

                    returns a list of nodes that have the sdfsFilename.
                    if the file does not exist (in the shared file system)
                    it replies with an empty list.
                */

                fmt.Printf("Received a get request %s\n", message)
                log.Printf("Received a get request %s\n",message)
                sdfsFilename := split_message[1]
                _, ok := fileMap[sdfsFilename]
                var nodes_str = ""

                if ok {
                    fileMap[sdfsFilename].nodeIds = removeDuplicates(fileMap[sdfsFilename].nodeIds)
                    for _, node := range(fileMap[sdfsFilename].nodeIds) {
                        nodes_str = nodes_str + strconv.Itoa(node) + ","
                    }
                    if len(nodes_str) > 0 {
                        nodes_str = nodes_str[:len(nodes_str)-1]
                    }
                }
                reply := fmt.Sprintf("getreply %s %s\n", sdfsFilename, nodes_str)
                log.Printf("Reply for message :%s is %s \n",message,reply)
                fmt.Fprintf(conn, reply)
                fmt.Printf("Sent a reply %s\n", reply)

            case "delete":
                /*
                    "delete sdfsFilename"

                    sends a deletefile message to the nodes that store sdfsFilename (in their shared directory).
                    Also update fileMap and nodeMap ;)
                */
                sdfsFilename := split_message[1]
                _, ok := fileMap[sdfsFilename]
                if ok {
                    fileMap[sdfsFilename].nodeIds = removeDuplicates(fileMap[sdfsFilename].nodeIds)
                    for _, nodeId := range(fileMap[sdfsFilename].nodeIds) {

                        go deleteFile(nodeId, sdfsFilename)

                        _, ok2 := nodeMap[nodeId][sdfsFilename]
                        if ok2 {
                            delete(nodeMap[nodeId], sdfsFilename)
                        } else {
                            fmt.Printf("nodeMap[%d] does not have %s file\n", nodeId, sdfsFilename)
                        }
                    }
                    delete(fileMap, sdfsFilename)

                    fmt.Fprintf(conn, "deleting ...\n") 
                } else {
                    fmt.Fprintf(conn, "file %s does not exist in the SDFS\n", sdfsFilename)
                }

            /*
            case "keyack":
               
                
                key := split_message[1]
                if keyStatus[key] != DONE{
                    keyCount = keyCount -1
                }
                keyStatus[key] = DONE
                log.Printf("keyack RECVD %s , remaining keys %d \n",key,keyCount)
                fmt.Printf("keyack RECVD %s , remaining keys %d \n",key,keyCount)

                fmt.Printf("%s key has been processed and the corresponding sdfs is added\n", key)

                success := true
                for tempKey := range keyStatus {
                    if keyStatus[tempKey] != DONE {
                        // fmt.Printf("!!!!!!!!!!!!!!!!!!!!!!!!!!!!Key %s not done \n",tempKey)
                        success = false
                        break
                    }
                }

                if success {
                    fmt.Printf("Maple task completed successfully\n")
                    mapleRunning = false
                    // clear all the data structures, maple, delete misc temp files
                    // for tempKey := range(keyStatus) {
                    //     fmt.Printf("%s ", tempKey)
                    // }
                    // fmt.Printf("\n")
                    //Clean the data structure

                    for k := range mapleId2Node {
                        delete(mapleId2Node, k)
                    }
                    for k := range mapleCompMap{
                        delete(mapleCompMap, k)
                    }
                    for k := range node2mapleJob {
                        delete(node2mapleJob, k)
                    }

                    workerNodes = nil

                    mapleBarrier = false

                    for k := range(keyMapleIdMap){
                        delete(keyMapleIdMap, k)
                    }

                    for k := range(keyStatus){
                        delete(keyStatus, k)
                    }

                    os.Exit(1)
                }
            */

            // case "ack": 
            //     /*
            //         "ack action srcNode destNode(s) sdfsFilename"

            //         if action is put, it means quorum has been reached 
            //         for file sdfsFilename and with destNodes. 
            //         now the master must send a confirmation to destNodes 
            //         to "movefile" from temp dir to shared dir. 
            //         Also update fileMap and nodeMap. 

            //         Similarly for replicate, the only difference is --  
            //         for put quorum is reached (for 4 nodes), whereas replicate
            //         means the file has been replicated to destNode (only one node).
            //     */

            //     action := split_message[1]
            //     srcNode, err := strconv.Atoi(split_message[2])
            //     if err != nil {
            //         log.Printf("[ME %d] Cannot convert %s to an int node id\n", myVid, split_message[2])
            //         break
            //     }
            //     destNodes_str := split_message[3]
            //     sdfsFilename := split_message[4]

            //     if action == "put" {
                    
            //         /*
            //             respond only if the quorum is for the lastest put on sdfsFilename
            //             if write-write conflict happens and the user replies yes;
            //             then the earlier quorum ack can be ignored 
            //             => respond only if the quorum ack corresponds to the last put on sdfsFilename
            //         */ 

            //         if conflictMap[sdfsFilename].id == srcNode {

            //             destNodes := removeDuplicates(string2List(destNodes_str))
            //             for _, node := range(destNodes) {
            //                 go sendConfirmation(node, sdfsFilename, srcNode)
            //             }

            //             updateTimestamp := time.Now().Unix()
            //             var newfiledata fileData 
            //             newfiledata.timestamp = updateTimestamp
            //             if len(destNodes) > 4{
            //                 destNodes = destNodes[:4]
            //             }
            //             newfiledata.nodeIds = destNodes
            //             fileMap[sdfsFilename] = &newfiledata

            //             for _, node := range(destNodes) {
            //                 _, ok := nodeMap[node]
            //                 if ok {
            //                     nodeMap[node][sdfsFilename] = updateTimestamp
            //                 } else {
            //                     nodeMap[node] = make(map[string]int64)
            //                     nodeMap[node][sdfsFilename] = updateTimestamp
            //                 }
            //             }
            //         } else {
            //             // ignore
            //         }
            //     } else if action == "replicate" {
            //         destNodes := removeDuplicates(string2List(destNodes_str))
            //         destNode := destNodes[0]

            //         go sendConfirmation(destNode, sdfsFilename, srcNode)
                    
            //         updateTimestamp := time.Now().Unix()
            //         if len(fileMap[sdfsFilename].nodeIds) < 4 {
            //             toAdd := true
            //             for _,elem := range fileMap[sdfsFilename].nodeIds{
            //                 if elem == destNode{
            //                     toAdd = false
            //                     break
            //                 }
            //             }
            //             if !toAdd{
            //                 break
            //             }
            //             fileMap[sdfsFilename].nodeIds = append(fileMap[sdfsFilename].nodeIds, destNode)
            //         }else{
            //             break
            //         }

            //         _, ok := nodeMap[destNode]
            //         if ok {
            //             nodeMap[destNode][sdfsFilename] = updateTimestamp
            //         } else {
            //             nodeMap[destNode] = make(map[string]int64)
            //             nodeMap[destNode][sdfsFilename] = updateTimestamp
            //         } 

            //         fmt.Printf("replicate %s complete, time now = %d\n", sdfsFilename, time.Now().UnixNano())                   
            //     }

            case "replace":
                /*
                    "replace oldnode sdfsFilename excludeList"
                    if some node dies in the middle of a `put` file (transfer)
                    initiator requests master for a new node other than the ones in excludeList.
                */
                excludeList_str := split_message[2]

                excludeList := string2List(excludeList_str)

                newNode := getRandomNodes(excludeList, 1)
                message := fmt.Sprintf("%d\n",newNode)
                fmt.Fprintf(conn, message)
            }
            conn.Close()
        }
    }
}


func sendConfirmation(subject int, sdfsFilename string, sender int) {
    timeout := 20 * time.Second

    ip := memberMap[subject].ip
    port := fileTransferPort

    acquireConn()
    conn, err := net.DialTimeout("tcp", ip + ":" + strconv.Itoa(port), timeout) 
    if err != nil {
        log.Printf("[ME %d] Unable to dial a connection to %d (to send confirmation for %s)\n", myVid, subject, sdfsFilename)
        releaseConn()
        return
    } 

    message := fmt.Sprintf("movefile %s %d", sdfsFilename, sender)
    padded_message := fillString(message, messageLength)
    conn.Write([]byte(padded_message))

    conn.Close()
    releaseConn()
    fmt.Printf("Sent a movefile %s request to %d\n", sdfsFilename, subject)
}

func string2List(given_str string) ([]int) {
    nodes := []int{}
    if len(given_str) == 0{
        return nodes
    }
    list_str := strings.Split(given_str, ",")
    for _, e := range list_str {
        node, err := strconv.Atoi(e)
        if err != nil {
            log.Printf("[ME %d] Cannot convert %s to an int\n", myVid, e)
        }
        nodes = append(nodes, node)
    }
    return nodes
}

func removeDuplicates( val []int) ([]int){
    valList:= []int{}
    mmap := make(map[int]bool)
    for _, elem := range(val){
        _,ok := mmap[elem]
        if !ok{
            mmap[elem] = true
            valList = append(valList,elem)
        }
    }
    return valList

}

func deleteFile(nodeId int, sdfsFilename string) {
    timeout := time.Duration(20) * time.Second

    ip := memberMap[nodeId].ip
    port := fileTransferPort

    acquireConn()
    conn, err := net.DialTimeout("tcp", ip + ":" + strconv.Itoa(port), timeout)
    if err != nil {
        log.Printf("[ME %d] Unable to dial a connection to %d (to delete file %s)\n", myVid, nodeId, sdfsFilename)
        releaseConn()
        return
    }

    message := fmt.Sprintf("deletefile %s", sdfsFilename)
    padded_message := fillString(message, messageLength)
    conn.Write([]byte(padded_message))

    conn.Close()
    releaseConn()

    log.Printf("[ME %d] Sent deletefile %s to %d\n", myVid, sdfsFilename, nodeId)
}

func getFile(nodeId int, sdfsFilename string, localFilename string) (bool) {

    timeout := time.Duration(20) * time.Second

    ip := memberMap[nodeId].ip
    port := fileTransferPort

    // connguard <- struct{}{}
    // fmt.Printf("[ME %d] Dialing ")

    if nodeId == myVid{
        // Get file from self
        success := copyFile(shared_dir + sdfsFilename, local_dir + localFilename)
        return success

    }

    acquireConn()
    conn, err := net.DialTimeout("tcp", ip + ":" + strconv.Itoa(port), timeout) 
    if err != nil {
        log.Printf("[ME %d] Unable to dial a connection to %d (to get file %s)\n", myVid, nodeId, sdfsFilename)
        releaseConn()
        return false
    }




    defer conn.Close()





    message := fmt.Sprintf("getfile %s", sdfsFilename)
    padded_message := fillString(message, messageLength)
    conn.Write([]byte(padded_message))

    bufferFileSize := make([]byte, 10)

    _, err = conn.Read(bufferFileSize)
    if err != nil {
        log.Printf("[ME %d] Error %s while fetching file %s from %d\n", myVid, err, sdfsFilename, nodeId)
        fmt.Printf("[ME %d] Error %s while fetching file %s from %d\n", myVid, err, sdfsFilename, nodeId)

        releaseConn()
        return false
    }

    fileSize, _ := strconv.ParseInt(strings.Trim(string(bufferFileSize), filler), 10, 64)

    log.Printf("[ME %d] Incoming file size %d", myVid, fileSize)
    fmt.Printf("[ME %d] Incoming file size %d", myVid, fileSize)

    acquireFile()
    file, err := os.Create(local_dir + localFilename)
    if err != nil {
        log.Printf("[ME %d] Cannot create the local file %s", myVid, localFilename) 
        releaseFile()
        return false
    }

    var receivedBytes int64
    success := false
    for {
        if (fileSize - receivedBytes) < BUFFERSIZE {
            bytesW, err := io.CopyN(file, conn, (fileSize - receivedBytes))
            receivedBytes += bytesW
            if err != nil {
                log.Printf("[ME %d] Cannot read from the connection to %d\n", myVid, nodeId)
                break
            }
            // fmt.Printf("%s %s -> filesize = %d,  total bytes received = %d\n", sdfsFilename, localFilename, fileSize, receivedBytes)
            success = true
            break
        }
        _, err = io.CopyN(file, conn, BUFFERSIZE)
        if err != nil {
            log.Printf("[ME %d] Cannot read from the connection to %d\n", myVid, nodeId)
            break
        }
        receivedBytes += BUFFERSIZE
    }

    if success {
        log.Printf("[ME %d] Successfully received file %s from %d\n", myVid, sdfsFilename, nodeId)
        fmt.Printf("[ME %d] Successfully received file %s from %d\n", myVid, sdfsFilename, nodeId)
    }

    file.Close()
    releaseFile()

    conn.Close()
    releaseConn()

    return success
}

func copyFile(srcFile string, destFile string) (bool){
 
    acquireFile()
    sfile, err := os.Open(srcFile)   
    if err != nil {
        // log.Panicf("failed reading file: %s", err)
        fmt.Printf("Error: %s, Could not open file %s for copying\n", err, srcFile)
        releaseFile()

        return false
    }

    input, err := ioutil.ReadAll(sfile)

    sfile.Close()
    releaseFile()

    if err != nil {
        log.Printf("[ME %d] Cannot read file from %s", srcFile)
        return false
    }

    acquireFile()
    err = ioutil.WriteFile(destFile, input, 0644)
    releaseFile()

    if err != nil {
        log.Printf("[ME %d] Cannot write file to %s", destFile)
        return false
    }

    return true
}

var ackTimeOut = 5

func replicateFile(nodeId int, sdfsFilename string) (bool) {
    timeout := 20 * time.Second

    ip := memberMap[nodeId].ip
    port := fileTransferPort

    acquireConn()
    conn, err := net.DialTimeout("tcp", ip + ":" + strconv.Itoa(port), timeout) 
    if err != nil {
        log.Printf("[ME %d] Unable to dial a connection to %d (to replicate file %s)\n", myVid, nodeId, sdfsFilename)
        
        releaseConn()
        return false
    }

    message := fmt.Sprintf("putfile %s %d", sdfsFilename, myVid)
    padded_message := fillString(message, messageLength)
    conn.Write([]byte(padded_message))

    acquireFile()
    f, err := os.Open(shared_dir + sdfsFilename)
    if err != nil {
        log.Printf("[ME %d] Cannot open shared file %s\n", sdfsFilename)
        
        releaseFile()

        conn.Close()
        releaseConn()

        return false
    }

    fileInfo, err := f.Stat()
    if err != nil {
        log.Printf("[ME %d] Cannot get file stats for %s\n", myVid, sdfsFilename)
        
        f.Close()
        releaseFile()

        conn.Close()
        releaseConn()

        return false
    }

    fileSize := fillString(strconv.FormatInt(fileInfo.Size(), 10), 10)
    log.Printf("[ME %d] Sending file %s of size %s to %d\n", myVid, sdfsFilename, fileSize, nodeId)
    fmt.Printf("[ME %d] Sending file %s of size %s to %d\n", myVid, sdfsFilename, fileSize, nodeId)

    conn.Write([]byte(fileSize))

    sendBuffer := make([]byte, BUFFERSIZE)

    success := true
    for {
        _, err = f.Read(sendBuffer)
        if err != nil {
            if err == io.EOF {
                break
            } else {
                log.Printf("[ME %d] Error while reading file %s\n", myVid, sdfsFilename)
                success = false
                break
            }
        }

        _, err = conn.Write(sendBuffer)
        if err != nil{
            log.Printf("[ME %d] Could not send %s file bytes to %d\n", myVid, sdfsFilename, nodeId)
            success = false
            break
        }

    }

    f.Close()
    releaseFile()

    if success {
        log.Printf("[ME %s] Successfully sent the file %s to %d, waiting for done\n", myVid, sdfsFilename, nodeId)
        fmt.Printf("[ME %s] Successfully sent the file %s to %d, waiting for done\n", myVid, sdfsFilename, nodeId)

        conn.SetReadDeadline(time.Now().Add(time.Duration(ackTimeOut) * time.Second))
        reader := bufio.NewReader(conn)
        ack, err := reader.ReadString('\n')
        if err != nil {
            log.Printf("[ME %d] Error while reading ACK from %d for %s file", myVid, nodeId, sdfsFilename)
            conn.Close()
            releaseConn()

            return false
        }

        conn.Close()
        releaseConn()
        if len(ack) == 0{
            return false
        }
        ack = ack[:len(ack)-1]
        if ack == "done" {
            fmt.Printf("Received done, sending replicate ack to master\n")
            destNode := []int{nodeId}
            sendAcktoMaster("replicate", myVid, list2String(destNode), sdfsFilename)

            return true
        } else {
            fmt.Printf("Did not receive a DONE ACK for replicate\n")
            return false
        }
    } else {
        fmt.Printf("Could not send file %s to %d, exiting\n", sdfsFilename, nodeId)
        conn.Close()
        releaseConn()

        return false
    }

}

func sendAcktoMaster(action string, srcNode int, destNodes string, fileName string) {
    ack_message := fmt.Sprintf("ack %s %d %s %s\n", action, srcNode, destNodes, fileName)

    timeout := time.Duration(20) * time.Second

    acquireConn()
    conn, err := net.DialTimeout("tcp", masterIP + ":" + strconv.Itoa(ackPort), timeout)
    if err != nil {
        log.Printf("[ME %d] Unable to connect with the master ip=%s port=%d", myVid, masterIP, ackPort)
        releaseConn()
        return
    }

    fmt.Fprintf(conn, ack_message)
    conn.Close()
    releaseConn()
    return
}

// var doneList = make([]int, 0, 4)

func sendFile(nodeId int, localFilename string, sdfsFilename string, wg *sync.WaitGroup, allNodes []int,ch chan<- int,tryCount int) {

    if nodeId<0{
        ch <- -1
        wg.Done()
        return
    }
    if tryCount<=0{
        ch <- -1
        wg.Done()
        return
    }


    fmt.Printf("In the sendFile sending file %s to node %d\n",localFilename,nodeId)

    if nodeId == myVid {
        success := copyFile(local_dir + localFilename, temp_dir + sdfsFilename + "." + strconv.Itoa(nodeId))

        if success {
            // doneList = append(doneList, nodeId)
            fmt.Printf("sent the file to %d\n", nodeId)
            ch <- nodeId
            wg.Done()
        }else{
            ch <- nodeId
            wg.Done()
        }

        return
    }

    timeout := 20 * time.Second

    port := fileTransferPort
    ip := memberMap[nodeId].ip

    retry := false
    fileOpenState := false
    connOpenStatus := false
    success := true

    acquireConn()
    connOpenStatus = true
    conn, err := net.DialTimeout("tcp", ip + ":" + strconv.Itoa(port), timeout) 
    if err != nil {
        log.Printf("[ME %d] Unable to dial a connection to %d (to send file %s)\n", myVid, nodeId, sdfsFilename)
        releaseConn()
        // wg.Done()
        // Can't return wg.Wait issues
        //// To Do Stuff , retry
        connOpenStatus = false
        retry = true
    }

    if !retry{

        message := fmt.Sprintf("putfile %s %d", sdfsFilename, myVid)
        padded_message := fillString(message, messageLength)
        _,err := conn.Write([]byte(padded_message)) // Remove error this part
        if err != nil{
            fmt.Printf("[ME %d] Error while sending putfile request for %s \n",myVid,sdfsFilename)
            conn.Close()
            releaseConn()
            // wg.Done()
            // Don't end retry
            // return
            retry = true
        }else{
            fmt.Printf("Sent a putfile %s request to %d\n", sdfsFilename, nodeId)
        }

    }
    if !retry{
        acquireFile()
        fileOpenState = true
        f, err := os.Open(local_dir + localFilename) 
        if err != nil {
            log.Printf("[ME %d] Cannot open local file %s\n", localFilename)
            
            releaseFile()

            conn.Close()
            releaseConn()
            ch <- -1
            wg.Done()

            // Can't return wg.Wait issues 
            // Retry

            return
        }

        fileInfo, err := f.Stat()
        if err != nil {
            log.Printf("[ME %d] Cannot get file stats for %s\n", myVid, localFilename)
            
            f.Close()
            releaseFile()

            conn.Close()
            releaseConn()

            ch <- -1
            wg.Done()
            //Can't return 
            // retyr

            return
        }
        

        if !retry{

            fileSize := fillString(strconv.FormatInt(fileInfo.Size(), 10), 10)
            log.Printf("[ME %d] Sending file %s of size %s to %d", myVid, localFilename, fileSize, nodeId)
            fmt.Printf("[ME %d] Sending file %s of size %s to %d", myVid, localFilename, fileSize, nodeId)

            _,err := conn.Write([]byte(fileSize))
            if err != nil{
                fmt.Printf("[ME %d] Error in sending fileSize %s\n",myVid,localFilename)
                
                f.Close()
                releaseFile()

                conn.Close()
                releaseConn()

                // wg.Done()
                fileOpenState = false
                connOpenStatus = false
                retry = true
                // Retyr
                // return


            }
        } 


        if !retry{

            sendBuffer := make([]byte, BUFFERSIZE)

            

            for {
                _, err = f.Read(sendBuffer)
                if err != nil {
                    if err == io.EOF {
                        break
                    } else {
                        success = false
                        log.Printf("[ME %d] Error while reading file %s", myVid, localFilename)
                        break
                    }
                }

                _, err = conn.Write(sendBuffer)
                if err != nil {
                    success = false
                    log.Printf("[ME %d] Could not send %s file bytes to %d", myVid, localFilename, nodeId)
                    // request another node to write this file from master
                    break
                }
            }
        } 

        if fileOpenState{
            f.Close()
            releaseFile()
            fileOpenState = false
        }



    }


    // success := true  

    success2 := true

    if success && !retry{
        log.Printf("[ME %s] Successfully sent the file %s to %d, now waiting for ACK\n", myVid, localFilename, nodeId)

        conn.SetReadDeadline(time.Now().Add(time.Duration(ackTimeOut) * time.Second))
        reader := bufio.NewReader(conn)
        ack, err := reader.ReadString('\n')
        if err != nil {
            success2 = false
            log.Printf("[ME %d] Error while reading ACK from %d for %s file\n", myVid, nodeId, sdfsFilename)
        } else {
            ack = ack[:len(ack)-1]
            if ack == "done" {
                // doneList = append(doneList, nodeId)
                fmt.Printf("Sent the file %s to %d\n", localFilename ,nodeId)
                ch <- nodeId
                wg.Done()
            } else {
                success2 = false
            }
        }
    }
    if connOpenStatus{
        conn.Close()
        releaseConn() 
    }

    if !success || !success2 || retry{
        newnode := replaceNode(nodeId, sdfsFilename, allNodes)
        for try:=0;try<3;try++{
            if newnode != -1 {
                break
            } else {
                newnode = replaceNode(nodeId, sdfsFilename, allNodes)
            }
        }
        allNodes = append(allNodes, newnode)
        
        go sendFile(newnode, localFilename, sdfsFilename, wg, allNodes,ch,tryCount-1)
    }


    return
}

func replaceNode(oldnode int, sdfsFilename string, excludeList []int) int {

    timeout := 20 * time.Second

    acquireConn()
    conn, err := net.DialTimeout("tcp", masterIP + ":" + strconv.Itoa(masterPort), timeout)
    if err != nil {
        log.Printf("[ME %d] Unable to connect with the master ip=%s port=%d", myVid, masterIP, masterPort)

        releaseConn()
        return -1
    }

    master_command := fmt.Sprintf("replace %d %s %s", oldnode, sdfsFilename, list2String(excludeList))
    fmt.Fprintf(conn, master_command)

    reader := bufio.NewReader(conn)
    reply, err := reader.ReadString('\n')
    if err != nil || len(reply) == 0 {
        
        conn.Close()
        releaseConn()

        return replaceNode(oldnode, sdfsFilename, excludeList)
    }

    conn.Close()
    releaseConn()

    newNode, err := strconv.Atoi(reply[:len(reply)-1])
    if err != nil {
        fmt.Printf("Cannot convert newnode reply to int\n")
    }
    return newNode

}

func ReadWithTimeout(reader *bufio.Reader, timeout time.Duration) (string, error) {
    s := make(chan string)
    e := make(chan error)
    go func() {
        line, err := reader.ReadString('\n')
        if err != nil {
            e <- err
        } else {
            s <- line
        }
        close(s)
        close(e)
    }()

    select {
        case line := <-s:
            return line, nil
        case err := <-e:
            return "", err
        case <-time.After(timeout):
            return "", errors.New("timeout")
    }
}

func executeCommand(command string, userReader *bufio.Reader) {

    timeout := 20 * time.Second

    acquireConn()
    conn, err := net.DialTimeout("tcp", masterIP + ":" + strconv.Itoa(masterPort), timeout)
    if err != nil {
        log.Printf("[ME %d] Unable to connect with the master ip=%s port=%d", myVid, masterIP, masterPort)
        releaseConn()
        return
    }
    defer releaseConn()
    defer conn.Close()

    // fmt.Printf("%s\n", command)

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
        if len(reply) == 0 {
            break
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
        initTime := time.Now()
        sdfsFilename := split_command[1]
        localFilename := split_command[2]

        master_command := fmt.Sprintf("get %s\n", sdfsFilename)
        fmt.Fprintf(conn, master_command)

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
            break
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
                break
            }
        }

        elapsed := time.Since(initTime)

        if !success {
            fmt.Printf("[ME %d] Could not fetch %s shared to %s local\n", myVid, sdfsFilename, localFilename)
        }

        fmt.Printf("Time taken for get %s\n", elapsed)

    case "delete":

        fmt.Fprintf(conn, command + "\n")
        log.Printf("[ME %d] Forwarded the %s command to the master\n", myVid, command)
        reader := bufio.NewReader(conn)
        reply, err := reader.ReadString('\n')
        if err!= nil{
            fmt.Println(err)
        } else{
            fmt.Printf("%s",reply)
        }

    // case "juice":

    //     if myIP != masterIP {
    //         fmt.Printf("Run Juice command from master\n")
    //         break
    //     }

    //     if (mapleRunning || juiceRunning) { 
    //         fmt.Printf("Already Running Maple-Juice\n")
    //         break
    //     }

    //     juiceInitTime = time.Now()

    //     juiceRunning = true

    //     juiceExeFile := split_command[1]
    //     numJuices, err := strconv.Atoi(split_command[2])
    //     if err != nil {
    //         fmt.Printf("Could not convert numJuices %s to int\n", split_command[2])
    //     }

    //     aliveNodeCount := getAliveNodeCount()
    //     numMaples = min(numJuices, aliveNodeCount-1)
    //     fmt.Printf("num of juices %d\n", numJuices)

    //     sdfsJuiceInterPrefix = split_command[3]
    //     sdfsDestName = split_command[4]

        // var deleteInput bool 
        // if split_command[5] == "1" {
        //     deleteInput = true
        // } else {
        //     deleteInput = false
        // }

    //     sdfsMapleExe = mapleExeFile
    //     PutFileWrapper(mapleExeFile, sdfsMapleExe, conn)
    //     fmt.Printf("Ran put file wrapper for %s %s\n", mapleExeFile, sdfsMapleExe)





    case "maple":
        /*
        maple maple_exe num_maples sdfs_intermediate sdfs_input
        */

        if myIP != masterIP {
            fmt.Printf("Run Maple command from master\n")
            break
        }
        // clear all the maple related maps
        if (mapleRunning || juiceRunning) {
            fmt.Printf("Already Running Maple-Juice\n")
            break
        }

        mapleInitTime = time.Now()

        mapleRunning = true

        mapleExeFile := split_command[1]    // mapleExe should be in local
        numMaples, err := strconv.Atoi(split_command[2])
        if err != nil {
            fmt.Printf("Could not convert numMaples %s to int\n", split_command[2])
        }
        aliveNodeCount := getAliveNodeCount()
        numMaples = min(numMaples, aliveNodeCount - 1)
        fmt.Printf("num of maples %d\n", numMaples)

        sdfsInterPrefix = split_command[3]
        mapleSrcPrefix := split_command[4]

        sdfsMapleExe = mapleExeFile
        PutFileWrapper(mapleExeFile, sdfsMapleExe, conn)
        fmt.Printf("Ran put file wrapper for %s %s\n", mapleExeFile, sdfsMapleExe)

        mapleFiles = []string{}
        for file := range fileMap {
            if strings.Contains(file, mapleSrcPrefix) {
                mapleFiles = append(mapleFiles, file)
            }
        }
        fmt.Printf("Maple files %v\n", mapleFiles)
        numMaples = min(numMaples, len(mapleFiles))

        workerNodes = getRandomNodes([]int{0}, numMaples)
        fmt.Printf("Maple Worker Nodes : %v \n",workerNodes)
        log.Printf("Maple Worker Nodes : %v \n",workerNodes)

        // time.Sleep(2 * time.Second)

        // fmt.Printf("Worker Nodes : %v \n",workerNodes)

        var mapleIdx = 0
        var nodeIdx = 0
        for {
            currNode := workerNodes[nodeIdx]
            if currNode != 0 {
                mapleId2Node[mapleIdx] = currNode
                mapleCompMap[mapleIdx] = false

                _, ok := node2mapleJob[currNode]
                if ok {
                    node2mapleJob[currNode].assignedMapleIds = append(node2mapleJob[currNode].assignedMapleIds, mapleIdx)
                } else {
                    var jobnode mapleJob
                    jobnode.assignedMapleIds = []int{mapleIdx}
                    jobnode.keysAggregate = []string{}
                    jobnode.keysGenerate = []string{}

                    node2mapleJob[currNode] = &jobnode
                }

                go sendMapleInfo(currNode, mapleIdx, sdfsMapleExe, mapleFiles[mapleIdx])
                mapleIdx = mapleIdx + 1
                if mapleIdx == len(mapleFiles) {
                    break
                }
            }
            nodeIdx = (nodeIdx + 1) % len(workerNodes)
        }


        fmt.Printf("Maple map %v\n", mapleId2Node)



    case "juice":
        /*
        maple maple_exe num_maples sdfs_intermediate sdfs_input
        */

        if myIP != masterIP {
            fmt.Printf("Run juice command from master\n")
            break
        }
        // clear all the maple related maps
        if (mapleRunning || juiceRunning) {
            fmt.Printf("Already Running Maple-Juice\n")
            break
        }

        sdfsDestJuiceName = split_command[4]
        jFileName := fmt.Sprintf("%s%s.juice",juice_dir,sdfsDestJuiceName)

        acquireFile()
        juicefileO, err:= os.Create(jFileName)
        if err != nil {
            fmt.Printf("Unable to create Juice File %s\n",jFileName)
            log.Printf("Unable to create Juice File %s\n",jFileName)
            break
        }
        juicefileO.Close()
        releaseFile()

        juiceInitTime = time.Now()

        juiceRunning = true

        juiceExeFile := split_command[1]    // mapleExe should be in local
        sdfsJuiceExe = juiceExeFile
        PutFileWrapper2(juiceExeFile, sdfsJuiceExe)
        LocjuicePath := fmt.Sprintf("%s%s",shared_dir,sdfsJuiceExe)
        _,err = os.Stat(LocjuicePath)
        if os.IsNotExist(err){
            log.Printf("Could not put JuiceExe : %s\n",juiceExeFile)
            fmt.Printf("Could not put JuiceExe : %s\n",juiceExeFile)
            PutFileWrapper2(juiceExeFile, sdfsJuiceExe)
            // break

        }
        _,err = os.Stat(LocjuicePath)
        if os.IsNotExist(err){
            log.Printf("Could not put Twice JuiceExe : %s\n",juiceExeFile)
            fmt.Printf("Could not put Twice JuiceExe : %s\n",juiceExeFile)
            juiceRunning = false
            break
        }
        fmt.Printf("Ran put file wrapper for %s %s\n", juiceExeFile, sdfsJuiceExe)
        log.Printf("Ran put file wrapper for %s %s\n", juiceExeFile, sdfsJuiceExe)


        numJuices, err := strconv.Atoi(split_command[2])
        if err != nil {
            fmt.Printf("Could not convert numJucies %s to int\n", split_command[2])
        }
        aliveNodeCount := getAliveNodeCount()
        numJuices = min(numJuices, aliveNodeCount - 1)
        fmt.Printf("num of juices %d\n", numJuices)

        sdfsJuiceInterPrefix = split_command[3]



        /*  ********** Will implement when required */
        // var deleteInput bool 
        // if split_command[5] == "1" {
        //     deleteInput = true
        // } else {
        //     deleteInput = false
        // }

        
        juiceFiles = []string{}
        for file := range fileMap {
            if strings.Contains(file, sdfsJuiceInterPrefix) {
                juiceFiles = append(juiceFiles, file)
            }
        }
        fmt.Printf("Juice files %v\n", juiceFiles)
        numJuices = min(numJuices, len(juiceFiles))

        juiceCount = len(juiceFiles)

        workerNodes = getRandomNodes([]int{0}, numJuices)

        fmt.Printf("Juice Worker Nodes : %v \n",workerNodes)
        log.Printf("Juice Worker Nodes : %v \n",workerNodes)

        // This shoould be in go routine so that we can send requests peacefully
        go func () {

            var juiceIdx = 0
            var nodeIdx = 0
            for {
                currNode := workerNodes[nodeIdx]
                if currNode != 0 {
                    juiceId2Node[juiceIdx] = currNode
                    juiceCompMap[juiceIdx] = ONGOING
                    juiceTimeStamp[juiceIdx] = time.Now().Unix()


                    _, ok := node2juiceJob[currNode]
                    if ok {
                        node2juiceJob[currNode].assignedJuiceIds = append(node2juiceJob[currNode].assignedJuiceIds, juiceIdx)
                    } else {
                        var jobnode juiceJob
                        jobnode.assignedJuiceIds = []int{juiceIdx}
                        jobnode.keysAggregate = []string{}
                        // jobnode.keysGenerate = []string{}

                        node2juiceJob[currNode] = &jobnode
                    }
                    acquireJuice()
                    go func(nodeID int, juiceId int, exeFile string, juicefile string) {
                        log.Printf("Sent SENTJUICE keyFile : %s to nodeID : %d with juiceID : %d\n",juicefile,nodeID,juiceId)
                        sendJuiceInfo(nodeID,juiceId,exeFile,juicefile)
                        releaseJuice()
                    }(currNode, juiceIdx, sdfsJuiceExe, juiceFiles[juiceIdx])
                    // sendJuiceInfo(currNode, juiceIdx, sdfsJuiceExe, juiceFiles[juiceIdx])
     
                    juiceIdx = juiceIdx + 1
                    if juiceIdx == len(juiceFiles) {
                        break
                    }
                }
                nodeIdx = (nodeIdx + 1) % len(workerNodes)
            }


            fmt.Printf("Juice map %v\n", juiceId2Node)
            
        }()  






    case "put":
        initTime := time.Now()
        localFilename := split_command[1] 

        _, err := os.Stat(local_dir + localFilename)
        if os.IsNotExist(err) {
            // fmt.Printf("Got a put for %s, but the file does not exist\n", localFilename)
            log.Printf("[ME %d] Got a put for %s, but the file does not exist\n", myVid, localFilename)
            break
        }

        sdfsFilename := split_command[2]

        master_command := fmt.Sprintf("put %s %d\n", sdfsFilename, myVid)
        fmt.Printf("%s\n", master_command)
        fmt.Fprintf(conn, master_command) // Sent the request to master

        reader := bufio.NewReader(conn) // Master response
        reply, err := reader.ReadString('\n')
        if err != nil {
            log.Printf(" Can't move forward with put reqest")
            break
        }
        reply = reply[:len(reply)-1]
        fmt.Printf("%s\n", reply)
        split_reply := strings.Split(reply, " ")
        
        /*
        No conflict in MP4
        if split_reply[0] == "conflict" {
            // Wait for user input
            // user_reader := bufio.NewReader(os.Stdin)
            // confResp, _ := userReader.ReadString('\n')
            confResp, err := ReadWithTimeout(userReader, 30 * time.Second)
            if err != nil {
                fmt.Printf("%s Please press enter to proceed\n", err)
                break
            }
            // fmt.Printf("This is the confResp: %s", confResp)

            _, err = fmt.Fprintf(conn, confResp)
            if err != nil{
                // Issue with the master
            }
            confResp = confResp[:len(confResp)-1]

            if confResp != "yes" {
                break
            }
            // read the new input from the master
            reply, err := reader.ReadString('\n')
            if err != nil{
                log.Printf(" Can't move forward with put reqest")
                break // Free up the user command
            }
            reply = reply[:len(reply)-1]
            fmt.Printf("%s\n", reply)
            split_reply = strings.Split(reply, " ")
        }
        */

        // conn.Close()
        // releaseConn()
        go func(){

            if len(split_reply[2]) == 0{
                return
            }
            nodeIds_str := strings.Split(split_reply[2], ",")
            nodeIds := []int{}
            for _, node_str := range nodeIds_str {
                node, err := strconv.Atoi(node_str)
                if err != nil {
                    panic(err)
                    // break // Free up the user 
                }
                if node >= 0{
                    nodeIds = append(nodeIds, node)
                }
            }
            if len(nodeIds) == 0{
                return
            }
            var wg sync.WaitGroup
            wg.Add(len(nodeIds))

            LocdoneList:=[]int{}
            myChan := make(chan int , len(nodeIds))

            for _, node := range nodeIds {

                // connguard <- struct{}{}
                go sendFile(node, localFilename, sdfsFilename, &wg, nodeIds,myChan,2)
            }

            wg.Wait()
            // Get All the 
            for i:= 0;i<len(nodeIds);i++{
                newVal := <-myChan
                if newVal < 0{
                    continue
                }else{
                    LocdoneList = append(LocdoneList,newVal)
                }
            }
            doneList_str := list2String(LocdoneList)
            sendAcktoMaster("put", myVid, doneList_str, sdfsFilename)

            elapsed := time.Since(initTime)

            fmt.Printf("Time taken for put %s\n",elapsed)

        }() 
        
    }
}

func scanCommands() {
    for {
        fmt.Printf("\n")
        reader := bufio.NewReader(os.Stdin)
        command, _ := reader.ReadString('\n')
        command = command[:len(command)-1]
        split_command := strings.Split(command, " ")
        command_type := split_command[0]
        switch command_type {
        case "children":
            printChildren()

        case "me":
            fmt.Printf("%d\n", myVid)

        case "monitors":
            printMonitors()

        case "members":
            printMembershipList()
        
        case "store":
            for fileName := range(fileTimeMap) {
                fmt.Printf("%s ", fileName)
            }

        case "keys":
            for tempKey := range keyStatus {
                if keyStatus[tempKey] != DONE {
                    fmt.Printf("Key %s not done \n",tempKey)
                    // success = false
                    // break
                }
            }
        case "keysd":
            for tempKey := range keyStatus {
                if keyStatus[tempKey] == DONE {
                    fmt.Printf("Key %s done \n",tempKey)
                    // success = false
                    // break
                }
            }

        case "ls", "get", "delete", "put", "maple", "juice":
            if command_type == "get" {
                if len(split_command) < 3 {
                    break
                }
            }

            executeCommand(command, reader)
            // fmt.Printf("Exited the executeCommand function \n")
        }
    }
}

func getRandomNodes(excludeList []int, count int) ([]int) {
    var success = true
    result := make(map[int]bool)

    for {
        success = true
        randomnode := rand.Intn(len(memberMap))

        _, ok := result[randomnode]
        if ok {
            continue
        }

        if !memberMap[randomnode].alive {
            continue
        }

        for _, excludenode := range(excludeList) {
            if randomnode == excludenode {
                success = false
                break
            }
        }
        if success {
            result[randomnode] = true
            if len(result) == count {
                keys := make([]int, 0, len(result))
                for k := range result {
                    keys = append(keys, k)
                }
                return keys
            }
        }
    }
}

func initiateReplica(fileName string, srcNode int, destNode int) {
    fmt.Printf("[ME %d] Asked node %d to replicate file %s on node %d\n",myVid,srcNode,fileName,destNode)
    timeout := time.Duration(20) * time.Second

    ip := memberMap[srcNode].ip
    port := fileTransferPort
    // connguard<-struct{}{}

    acquireConn()
    conn, err := net.DialTimeout("tcp", ip + ":" + strconv.Itoa(port), timeout)
    if err != nil {
        log.Printf("[ME %d] Unable to dial a connection to %d (to replicate file %s)\n", myVid, srcNode, fileName)
        releaseConn()
        return
    }
    defer releaseConn()
    defer conn.Close()

    message := fmt.Sprintf("replicatefile %s %d", fileName, destNode)
    padded_message := fillString(message, messageLength)
    conn.Write([]byte(padded_message))

    log.Printf("[ME %d] Sent replicate %s file message from %d (at %d)\n", myVid, fileName, srcNode, destNode)

    return
}

func replicateFiles(subjectNode int) {
    // This function tries to make a replica of all files stored by subjectNode.

    fmt.Printf("CRASH \n")
    fmt.Printf("Replication started for node : %d, timestamp : %d\n",subjectNode,time.Now().UnixNano())

    for fileName, _ := range nodeMap[subjectNode] {
        // Remove from the fileMap node list
        // fmt.Printf("Inside replicate on crash of %d: filename = %s\n", subjectNode, fileName)
        filenodes := fileMap[fileName].nodeIds
        idx := -1
        for i, node := range filenodes {
            if node == subjectNode {
                idx = i
                break
            }
        }
        // fmt.Printf("idx = %d, len of filenodes = %d\n", idx, len(filenodes))
        if (idx == -1 || len(filenodes) == 0) {
            continue
        }
        filenodes[idx] = filenodes[len(filenodes)-1]
        filenodes = filenodes[:len(filenodes)-1]
        fileMap[fileName].nodeIds = filenodes

        newnode := getRandomNodes(filenodes, 1)
        fmt.Printf("File Name %s srcNode : %v , destNode : %v \n",fileName,filenodes,newnode)
        // just checking for handling errors
        if len(filenodes) > 0{
            acquireParallel()
            go func (fileName string, srcNode int , destNode int) {
                initiateReplica(fileName, srcNode, destNode)
                releaseParallel()
            }(fileName, filenodes[0], newnode[0])
            // go 
        }
    }
    delete(nodeMap, subjectNode)
}


// this function needs work more 
func HandleFileReplication() {
    for{
        time.Sleep(time.Duration(replicatePeriod) * time.Second)

        if myIP == masterIP {
            // Run the code

            for fileName, _ := range(fileMap){
                filenodes := fileMap[fileName].nodeIds
                currTime := time.Now().Unix()
                if (fileMap[fileName].timestamp + int64(replicatePeriod)) < currTime{
                    continue
                }
                if len(filenodes) < 4 { 
                    if len(filenodes) == 0 {
                        continue
                    }
                    fmt.Printf("+++++++++++++++++++++++++++++++++++++++++++")
                    fmt.Printf("[ME %d] Handling file Replication for %s\n",myVid,fileName)
                    fmt.Printf("+++++++++++++++++++++++++++++++++++++++++++")
                    newnodes := getRandomNodes(filenodes, 4 - len(filenodes))
                    fmt.Printf("Init vector %v , Added nodes %v\n",filenodes, newnodes)
                    for _, newnode := range(newnodes) {

                        acquireParallel()
                        go func (fileName string, srcNode int , destNode int) {
                            initiateReplica(fileName, srcNode, destNode)
                            releaseParallel()
                        }(fileName, filenodes[0], newnode)
                        // go initiateReplica(fileName, filenodes[0], newnode)
                    }
                }
            }

        }
    }
}



//Disabled leader election for now

func LeaderHandleFileReplication() {
    if myIP == masterIP {
        // Run the code
        for fileName, _ := range(fileMap){
            filenodes := fileMap[fileName].nodeIds
            if len(filenodes) < 4 { 
                fmt.Printf("[ME %d] Handling file Replication for %s\n",myVid,fileName)
                newnodes := getRandomNodes(filenodes, 4 - len(filenodes))
                fmt.Printf("Init vector %v , Added nodes %v\n",filenodes, newnodes)
                for _, newnode := range(newnodes) {
                    acquireParallel()
                    go func (fileName string, srcNode int , destNode int) {
                        initiateReplica(fileName, srcNode, destNode)
                        releaseParallel()
                    }(fileName, filenodes[0], newnode)

                    // go initiateReplica(fileName, filenodes[0], newnode)
                }
            }
        }
    } 
}


func LeaderElection() {

    if ongoingElection == true{
        return
    }
    ongoingElection = true
    // Could wait for some second to let the node stabilize
    // the process
    fmt.Printf("[ME %d] Leader Election started \n",myVid)
    time.Sleep(3*time.Second)
    predId := getPredecessor(myVid)
    succId := getSuccessor(myVid)

    if myVid < predId && myVid < succId {

        fmt.Printf("[ME %d] I'm the leader \n",myVid)
        // I'm the leader
        masterIP = myIP // set my IP as the master IP
        masterPort = masterPort+1
        masterNodeId = myVid


        go listenMasterRequests()

        time.Sleep(1*time.Second)
        

        // Move your own things to fileMap and nodeMap
        for fileName := range(fileTimeMap){
            // Handling the fileName part
            // fileMap[fileName].nodeIds = removeDuplicates(fileMap[fileName].nodeIds)
            _,ok := fileMap[fileName]
            if ok{
                noAdd := true
                for _, elem := range fileMap[fileName].nodeIds{
                    if elem == myVid{
                        noAdd = false
                        break
                    }
                }
                if !noAdd{
                    continue
                }
                if len(fileMap[fileName].nodeIds) < 4{
                    fileMap[fileName].nodeIds = append(fileMap[fileName].nodeIds,myVid) 
                    fileMap[fileName].timestamp = fileTimeMap[fileName]
                }
            }else{
                var newfiledata fileData 
                newfiledata.timestamp = fileTimeMap[fileName]
                newfiledata.nodeIds =  removeDuplicates(string2List(strconv.Itoa(myVid)))
                fileMap[fileName] = &newfiledata
            }

            // Handle the nodeMap one
            _,ok = nodeMap[myVid]
            if ok{
                nodeMap[myVid][fileName] = fileTimeMap[fileName]
            }else{
                nodeMap[myVid] = make(map[string]int64)
                nodeMap[myVid][fileName] = fileTimeMap[fileName]
            }

        }


        ongoingElection = false
        electiondone <- true  
        fmt.Printf("[ME %d] New Master Elected %d\n",myVid,myVid)
                      

        leaderMsg := fmt.Sprintf("LEADER,%d,%d",myVid,masterPort)
        disseminate(leaderMsg)
        

        time.Sleep(1*time.Second)
        go LeaderHandleFileReplication()
        go HandleFileReplication()
    }

    // send everyone the leadership message
    // Can use disseminate
    
    



 


    // Finish by doing
    // ongoingElection = false
    // electiondone <- true   
    return
}

func LeaderHandler( subject int, newPort int) {

    if masterIP == memberMap[subject].ip{
        return
    }

    masterIP = memberMap[subject].ip
    masterPort = newPort
    masterNodeId = subject
    // Then start tranferring your own nodes to everyone in a go routuine
    go func(subject int) {
        // Start a tcp connection with the master
        leaderSucc := true
        timeout := time.Duration(20) * time.Second


        // connguard<- struct{}{}
        acquireConn()
        conn, err := net.DialTimeout("tcp", masterIP + ":" + strconv.Itoa(masterPort), timeout)
        if err != nil{
            fmt.Printf("[ME %d]Unable to connect to new Master %d \n",myVid,subject)
            // conn.Close()
            releaseConn()

        }
        defer releaseConn()
        defer conn.Close()

        // Send your fileTimeMap for
        message:= fmt.Sprintf("file %d\n",myVid)
        _,err = fmt.Fprintf(conn,message)
        if err!= nil{
            leaderSucc = false
        }
        for fileName := range(fileTimeMap){
            message:= fmt.Sprintf("fileName %d %s %d\n",myVid,fileName,fileTimeMap[fileName])
            _, err = fmt.Fprintf(conn,message)
            if err != nil{
                fmt.Println(err)
                leaderSucc = false
                break
            }

        }
        if leaderSucc{
            message:= fmt.Sprintf("done %d\n",myVid)
            _, err = fmt.Fprintf(conn,message)
            fmt.Printf("[ME %d] New Master Elected %d\n",myVid,subject)
        } 
        ongoingElection = false
        electiondone <- true   

        return

    }(subject)

    return
}


func JuiceRerunHandler(){
    for {
        time.Sleep(time.Duration(replicatePeriod) * time.Second)

        if myIP == masterIP {
            for tempKey := range juiceCompMap {
                if juiceCompMap[tempKey] == ONGOING {
                    if  (time.Now().Unix() -  juiceTimeStamp[tempKey]) > 30 {
                        // rerun the key
                        fmt.Printf("Key Rerun : %s \n",tempKey)
                        log.Printf("Key Rerun : %s \n",tempKey)

                        // go ProcessKey (tempKey, workerNodes[rand.Intn(len(workerNodes))], keyMapleIdMap[tempKey])
                        acquireJuice() // would block if guard channel is already filled
                        go func(nodeID int, juiceId int, JuiceExe string , inpFile string) {
                            log.Printf("Sent SENTJUICE keyFile : %s to nodeID : %d with juiceID : %d\n",inpFile,nodeID,juiceId)

                            sendJuiceInfo(nodeID, juiceId, JuiceExe,inpFile)
                            releaseJuice()
                        }(workerNodes[rand.Intn(len(workerNodes))], tempKey, sdfsJuiceExe,juiceFiles[tempKey])
                    }
                }
            }
        }
    }
} 


func keyRerunHandler(){
    for {
        time.Sleep(time.Duration(replicatePeriod) * time.Second)

        if myIP == masterIP {
            for tempKey := range keyStatus {
                if keyStatus[tempKey] == ONGOING {
                    if  (time.Now().Unix() -  keyTimeStamp[tempKey]) > 30 {
                        // rerun the key
                        fmt.Printf("Key Rerun : %s \n",tempKey)
                        log.Printf("Key Rerun : %s \n",tempKey)

                        // go ProcessKey (tempKey, workerNodes[rand.Intn(len(workerNodes))], keyMapleIdMap[tempKey])
                        acquireParallel() // would block if guard channel is already filled
                        go func(key string, respNode int, mapleIds []int) {
                            ProcessKey(key, respNode, mapleIds)
                            releaseParallel()
                        }(tempKey, workerNodes[rand.Intn(len(workerNodes))], keyMapleIdMap[tempKey])
                    }
                }
            }
        }
    }
} 



