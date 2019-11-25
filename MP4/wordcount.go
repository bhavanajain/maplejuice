package main

import (
	"bufio"
	"fmt"
	"strings"
	"os"
	"flag"
	"io"
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
	wordCount := make(map[string]int)
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
			lineWords := strings.Split(line, " ")
			for _, word := range lineWords {
				_, ok := wordCount[word]
				if ok {
					wordCount[word] += 1
				} else {
					wordCount[word] = 1
				}
			}
		}
		if fileEnd {
			break
		}
	}

	for word, count := range(wordCount) {
		fmt.Printf("%s %d\n", word, count)
	}
}