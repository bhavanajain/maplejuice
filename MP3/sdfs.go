package main

import (
	"os"
)

var head = 0
const BUFFERSIZE = 1024
var fileRecvPort int = 8080

func selectNodes() {
	var nodeIds [7]int
	var num_members int = len(members)
	for i := range(7) {
		nodeIds[i] = (head + i) % num_members
	}
	head = head + 1
	return nodeIds
}

func sendFile(nodeid int, filename str) {

	// [TODO] Add checksum: https://gobyexample.com/sha1-hashes

	timeout := time.Duration(20) * time.Second

	ip = members[nodeid].ip
	port = fileRecvPort

	conn, err := net.DialTimeout("tcp", ip + ":" + port, timeout)
	if err != nil {
		log.Printf("[File Sender] Unable to connect with %s %s\n", nodeid, filename)
		return
	}
	defer conn.Close()

	f, err := os.Open(filename)
	if err != nil {
		log.Printf("[File Sender]: Can't open file\n")
		return
	}

	fileInfo, err := f.Stat()
	if err != nil {
		log.Printf("[File Sender] Can't access file stats %s %s\n", nodeid, filename)
		return
	}

	fileSize := fillString(strconv.FormatInt(fileInfo.Size(), 10), 10)
	fileName := fillString(fileInfo.Name(), 64)

	log.Printf("[File Sender] filesize %s filename %s", fileSize, fileName)

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
	log.Printf("[File Sender] Completed sending the file %s %s\n", nodeid, filename)

	// Wait for an ack

	return
}

func recvFile() {

	// [TODO] Handle checksum

	ln, _ := net.Listen("tcp", ":" + fileRecvPort)
	for {
		conn, _ := ln.Accept()
		log.Println("[File Server] Accepted a new connection")	

		bufferFileName := make([]byte, 64)
		bufferFileSize := make([]byte, 10)

		conn.Read(bufferFileSize)
		fileSize, _ := strconv.ParseInt(strings.Trim(string(bufferFileSize), ":"), 10, 64)
	
		conn.Read(bufferFileName)
		fileName := strings.Trim(string(bufferFileName), ":")

		log.Println("[File Server] Read filename", fileName, "filesize" ,fileSize)

		file, err := os.Create(fileName)
		if err != nil {
			log.Println("[File Server Error] Couldn't create the file")	
		}
		defer file.Close()

		var receivedBytes int64
		for {
			if (fileSize - receivedBytes) < BUFFERSIZE {
				io.CopyN(file, conn, (fileSize - receivedBytes))
				break
			}
			io.CopyN(file, conn, BUFFERSIZE)
			receivedBytes += BUFFERSIZE
		}

		log.Println("[File Server] Completed reading file bytes")

		// TODO: Send an acknowledgement that I have received the file

		conn.Close()
	}
}

func readFile() {
	// get metadata for the required file -- nodes on which the file exists
	// query those 7 nodes and wait for 4 ACKs and read from the one with the latest timestamp
}

func writeFile() {
	// Think something RPC

	// send the file to master
}

func writeFileWithQuorum() {

	// master listens for write requests on one particular port
	// Once it receives a request, it gets the nodeids to store this file on
	// Starts sending the file and waits for ack from 4
	// once ack from 4 is received, update the lastest write timestamp for that file

}

// Other notes:
// Incorporate MP2 into this: 
// Send heartbeats via UDP and send the membership changes via TCP connection


	