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
)

func client() {
	ln, _ := net.Listen("tcp", ":8080")
	for {
		conn, _ := ln.Accept()
		defer conn.Close()

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
	}
}

func main() {
	client()
}