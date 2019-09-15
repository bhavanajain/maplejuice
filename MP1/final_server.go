package main

import (
	"net"
	"os"
	"bufio"
	"regexp"
	"fmt"
	"io"
	"strings"
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

		r, _ := regexp.Compile(pattern)
		file, err := os.Open(filename)
		defer file.Close()
		if err != nil {
			fmt.Println("[Error] Couldn't open the file", filename)
			break
		}
		
		file_reader := bufio.NewReader(file)
		var linenum int = 0
		var num_matches int = 0
		
		for {
			line, err := file_reader.ReadString('\n')
			linenum += 1
			if r.MatchString(line) {
				if line[len(line) - 1] != '\n'{
					line = line + "\n"
				}
				matched_line = fmt.Sprintf("%d$$$$%s", linenum, line)
				num_matches += 1
				fmt.Fprintf(conn, matched_line)
			}
			if err != nil {
				if err != io.EOF {
					fmt.Printf("[Error] Unknown error while reading file", filename)
				}
				break
			}
		}
		closing := num_matches + "," + "<<EOF>>\n"
		fmt.Fprintf(conn, closing)
		fmt.Printf("[Info] Completed sending line matches of", pattern, "in", filename)
		conn.Close()
	}
}

func main() {
	Server()
}