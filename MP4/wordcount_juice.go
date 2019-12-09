package main

import (
	"bufio"
	"fmt"
	"strings"
	"os"
	"flag"
	"io"
	"strconv"
)

func main() {
	inputFile := flag.String("inputfile", "", "path to the input file")
	flag.Parse()

	f, err := os.Open(*inputFile)
	if err != nil {
		fmt.Printf("Cannot open %s for reading\n", inputFile)
	}
	defer f.Close()

	fileReader := bufio.NewReader(f)
	result := make(map[string]int)
	fileEnd := false
	for {
		line, err := fileReader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				fmt.Printf("Unknown error encountered while reading file\n")
				break
			}
			fileEnd = true
		}
		if len(line) > 0 {
			if line[len(line)-1] == '\n' {
				line = line[:len(line)-1]
			}

			splitLine := strings.Split(line, " ")
			key := splitLine[0]
			value_str := splitLine[1]
			value, err := strconv.Atoi(value_str)
			if err != nil {
				panic(err)
			}

			_, ok := result[key]
			if ok {
				result[key] += value
			} else {
				result[key] = value
			}
		}
		if fileEnd {
			break
		}
	}

	for key, value := range (result) {
		fmt.Printf("%s-%d\n", key, value)
	}

}