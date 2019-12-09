package main

import (
	"bufio"
	"fmt"
	"strings"
	"os"
	"flag"
	"io"
	// "regexp"
)

const alpha = "abcdefghijklmnopqrstuvwxyz"
func alphaOnly(s string) bool {
   for _, char := range s {  
      if !strings.Contains(alpha, string(char)) {
         return false
      }
   }
   return true
}

// var IsAlpha = regexp.MustCompile(`^[a-zA-Z]+$`).MatchString

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
				resWord := strings.ToLower(word)
				// if !alphaOnly(word){
				// 	continue
				// }
				if !alphaOnly(resWord) {
					continue
				}
				_, ok := wordCount[resWord]
				if ok {
					wordCount[resWord] += 1
				} else {
					// if len(wordCount) < 3000 {
					wordCount[resWord] = 1
					// }
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