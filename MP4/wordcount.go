package main

import (
	"bufio"
	"fmt"
	"strings"
	"os"
	"flag"
	"io"
)

const alpha = "abcdefghijklmnopqrstuvwxyz"
func alphaOnly(s string) bool {
   for _, char := range s {  
      if !strings.Contains(alpha, strings.ToLower(string(char))) {
         return false
      }
   }
   return true
}
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
				if len(word) == 0 {
					continue
				}
				if !alphaOnly(word){
					continue
				}
				_, ok := wordCount[word]
				if ok {
					wordCount[word] += 1
				} else {
					if len(wordCount) < 2000 {
						wordCount[word] = 1
					}
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