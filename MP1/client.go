package main

import (
	"net"
	"os"
	"fmt"
	"strings"
	"bufio"
	"regexp"
	"io"
)

func grepOnFile(filepath string, pattern string) []string {
    r, _ := regexp.Compile(pattern)
    file, err := os.Open(filepath)
    defer file.Close()

    var test = []string{}

    if err != nil {
        fmt.Printf("Some error while reading file\n")
        return test
    }

    reader := bufio.NewReader(file)
    var line string
    var linenum int = 0
    var pattern_matches []string

    for {
        line, err = reader.ReadString('\n')

        if r.MatchString(line) {
            match := fmt.Sprintf("%d:%s", linenum, line)
            pattern_matches = append(pattern_matches, match)
        }
        linenum += 1
        if err != nil {
            break
        }
    }
    if err != io.EOF {
        fmt.Printf(" > Failed!: %v\n", err)
    }
    return pattern_matches
}

func client(){
	// This receives a connection from a server and replies whether it is alive or not
	// fmt.Printf("Waiting for conn")

	ln, _ := net.Listen("tcp", ":8080")
	for {
		conn, _ := ln.Accept()
		fmt.Printf("Got a new connection")
		reader := bufio.NewReader(conn)

		parameters,_ := reader.ReadString('\n')
		parameters = parameters[:len(parameters)-1]
		parameters_list := strings.Split(parameters, ",")
		filename, pattern := parameters_list[0], parameters_list[1]
		pattern_matches := grepOnFile(filename, pattern)
		for _, line_match := range pattern_matches {
			fmt.Fprintf(conn, line_match + "\n")
		}
		fmt.Fprintf(conn, "EOF" + "\n")
		conn.Close()
	}

	
}

func main() {
	client()	
}

