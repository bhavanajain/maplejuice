package main

import (
	"net"
	"os"
	"bufio"
	"regexp"
	"fmt"
	"io"
	"log"
	"strings"
	"strconv"
)

func client() {
	ln, _ := net.Listen("tcp", ":8080")
	for {
		conn, _ := ln.Accept()
		// defer conn.Close()

		conn_reader := bufio.NewReader(conn)

		params, _ := conn_reader.ReadString('\n')
		params = params[:len(params)-1]
		params_list := strings.Split(params, ",")
		filename, pattern := params_list[0], params_list[1]

		r, _ := regexp.Compile(pattern)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal("Error: Couldn't access the file\n")
		}
		defer file.Close()

		file_reader := bufio.NewReader(file)

		var linenum int = 0
		for {
			line, err := file_reader.ReadString('\n')
			linenum += 1
			if r.MatchString(line) {
				matched_line := fmt.Sprintf("%d$$$$%s", linenum, line)
				fmt.Fprintf(conn, matched_line)
			}
			if err != nil {
				if err != io.EOF {
					fmt.Printf("Unknown error while reading file %s", filename)
				}
				break
			}
		}
		fmt.Fprintf(conn, "<EOF>" + "\n")
		conn.Close()
		break
	}
	ln.Close()
}

func file_recv() {
	server, err := net.Listen("tcp", ":27001")
	if err != nil {
		panic(err)
	}
	defer connection.Close()

	connection, err := server.Accept()

	if err !=nil{
		fmt.Printf("Unable to accept")
	}
	defer connection.Close()
	fmt.Println("Connected to server, start receiving the file name and file size")
	bufferFileName := make([]byte, 64)
	bufferFileSize := make([]byte, 10)
	
	connection.Read(bufferFileSize)
	fileSize, _ := strconv.ParseInt(strings.Trim(string(bufferFileSize), ":"), 10, 64)
	
	connection.Read(bufferFileName)
	fileName := strings.Trim(string(bufferFileName), ":")
	
	newFile, err := os.Create(fileName)
	
	if err != nil {
		panic(err)
	}
	defer newFile.Close()
	var receivedBytes int64
	
	for {
		if (fileSize - receivedBytes) < BUFFERSIZE {
			io.CopyN(newFile, connection, (fileSize - receivedBytes))
			connection.Read(make([]byte, (receivedBytes+BUFFERSIZE)-fileSize))
			break
		}
		io.CopyN(newFile, connection, BUFFERSIZE)
		receivedBytes += BUFFERSIZE
	}
	fmt.Println("Received file completely!")
}
const BUFFERSIZE = 1024


func Test1() {
	fmt.Printf("Test1")
	file_recv()
	fmt.Printf("Client Started Waiting for grep")
	client()
}


func main() {
	//client()
	Test1()
	Test1()
	Test1()
}