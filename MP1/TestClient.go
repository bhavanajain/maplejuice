package main

import (
	"net"
	"os"
	"bufio"
	"regexp"
	"fmt"
	"io"
	"strings"
	"strconv"
)


func Server() {
	ln, _ := net.Listen("tcp", ":8080")
	for {
		conn, _ := ln.Accept()
		fmt.Println("[Info] Accepted a new connection")		

		conn_reader := bufio.NewReader(conn)

		params, _ := conn_reader.ReadString('\n')
		params = params[:len(params)-1]
		params_list := strings.Split(params, ",")
		filename, pattern := params_list[0], params_list[1]
		fmt.Println("[Info] filename: ", filename, "pattern: ", pattern)

		r, err := regexp.Compile(pattern)
		if err != nil {
			fmt.Println("[Error] regexp cannot compile the pattern")
			conn.Close()
			continue
		}
		file, err := os.Open(filename)
		if err != nil {
			fmt.Println("[Error] Couldn't open the file", filename)
			conn.Close()
			continue
		}
		defer file.Close()
		
		file_reader := bufio.NewReader(file)
		var linenum int = 0
		var num_matches int = 0
		
		for {
			line, err := file_reader.ReadString('\n')
			linenum += 1
			if len(line) > 0 && r.MatchString(line) {
				if line[len(line) - 1] != '\n'{
					line = line + "\n"
				}
				matched_line := fmt.Sprintf("%d$$$$%s", linenum, line)
				num_matches += 1
				fmt.Fprintln(conn, matched_line[:len(matched_line)-1])
				// fmt.Println("[Info] packet: %s", matched_line)
			}
			if err != nil {
				if err != io.EOF {
					fmt.Println("[Error] Unknown error while reading file", filename)
				}
				break
			}
		}
		closing := fmt.Sprintf("%d,<<EOF>>\n", num_matches)
		fmt.Fprintf(conn, closing)
		fmt.Println("[Info] Completed sending line matches of", pattern, "in", filename)
		conn.Close()
		break
	}
	// ln.Close()
}
func file_recv() {
	server, err := net.Listen("tcp", ":27001")
	if err != nil {
		panic(err)
	}
	defer server.Close()

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
	return
}
const BUFFERSIZE = 1024


func Test1() {
	fmt.Printf("Test1")
	file_recv()
	fmt.Printf("Client Started Waiting for grep")
	Server()
}


func main() {
	//client()
	Test1()
	// Test1()
	// Test1()
}