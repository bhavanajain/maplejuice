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
	"sync"
)

const BUFFERSIZE = 1024

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
		fmt.Println("[Info] Completed sending", num_matches, "line matches of", pattern, "in", filename)
		conn.Close()
	}
}

func ReceiveFile() {
	ln, _ := net.Listen("tcp", ":8081")
	for {
		conn, _ := ln.Accept()
		fmt.Println("[File Server] Accepted a new connection")	

		bufferFileName := make([]byte, 64)
		bufferFileSize := make([]byte, 10)

		conn.Read(bufferFileSize)
		fileSize, _ := strconv.ParseInt(strings.Trim(string(bufferFileSize), ":"), 10, 64)
	
		conn.Read(bufferFileName)
		fileName := strings.Trim(string(bufferFileName), ":")

		fmt.Println("[File Server] Read filename", fileName, "filesize" ,fileSize)

		file, err := os.Create(fileName)
		if err != nil {
			fmt.Println("[File Server Error] Couldn't create the file")	
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

		fmt.Println("[File Server] Completed reading file bytes")
		conn.Close()
	}
}

func main() {
	var wg sync.WaitGroup
	wg.Add(2)

	go ReceiveFile()
	go Server()

	wg.Wait()


}
