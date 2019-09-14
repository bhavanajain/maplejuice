package main

import (
	"bufio"
	"strings"
	"strconv"
	"flag"
	"os"
	"fmt"
	"log"
	"io"
	"sync"
	"time"
	"net"
	"regexp"
	"github.com/gookit/color"
)

var mutex = &sync.Mutex{}

func parseServerFile(serverFile string) map[string]int {
	serverMap := make(map[string]int)

	file, err := os.Open(serverFile)
	defer file.Close()

	if err != nil {
		log.Fatal(err)
	}

	reader := bufio.NewReader(file)
	var line string
	for {
		line, err = reader.ReadString('\n')
		if err != nil {
			break
		}
		// strip off the last character '\n'
		line = line[:len(line)-1]
		split_line := strings.Split(line, " ")
		serverMap[split_line[0]], _ = strconv.Atoi(split_line[1])	
	}

	if err != io.EOF {
		fmt.Println("Error while reading file")
	}

	return serverMap
}

func distributedGrep(serverMap map[string]int, pattern string, filePrefix string, terminal bool) {
	var wg sync.WaitGroup
	num_servers := len(serverMap)
	wg.Add(num_servers)

	for serverIP, fileIdx := range(serverMap) {
		go patternMatch(serverIP, pattern, fileIdx, filePrefix, terminal, &wg)
	}

	wg.Wait()
}

func patternMatch(serverIP string, pattern string, fileIdx int, filePrefix string, terminal bool, wg *sync.WaitGroup) {
	defer wg.Done()

	timeout := time.Duration(20) * time.Second
	filename := fmt.Sprintf("%s%d.log", filePrefix, fileIdx)

	conn, err := net.DialTimeout("tcp", serverIP + ":8080", timeout)
	if err != nil {
		fmt.Printf("Error connecting with client %s")
		return
	}
	defer conn.Close()

	magenta := color.FgMagenta.Render
    bold := color.OpBold.Render

    r, _ := regexp.Compile(pattern)

	for {
		parameters := filename + "," + pattern + "\n"
		fmt.Fprintf(conn, parameters)

		reader := bufio.NewReader(conn)
		var done bool = false

		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				fmt.Printf("Server %s has crashed", serverIP)
				done = true
				break
			}
			if strings.Contains(line, "<EOF>") {
				done = true
				break
			}

			split_line := strings.Split(line, ":")
			linenum_str, line := split_line[0], split_line[1]
			linenum, _ := strconv.Atoi(linenum_str)

			mutex.Lock()

			fmt.Printf("[%s] %d: ", filename, linenum)
			indices_grid := r.FindAllStringIndex(line, -1)
			next_start := 0
			for _, indices := range indices_grid {
				fmt.Printf("%s%s", line[next_start:indices[0]], bold(magenta(line[indices[0]:indices[1]])))
				next_start = indices[1]
			}
			fmt.Printf("%s\n", line[next_start:])

			mutex.Unlock()
		}
		if (done) {
			break
		}
	}	
	return
}

func main(){
	serverFile := flag.String("server_file", "servers.in", "File containing the IP and idx for distributed machines")
	pattern := flag.String("pattern", "^[0-9]*[a-z]{5}", "regexp pattern to match in distributed files")
	filePrefix := flag.String("file_prefix", "vm", "prefix of the file names on distributed machines")
	terminal := flag.Bool("terminal", true, "print output on terminal if true else store in separate files")

	flag.Parse()

	serverMap := parseServerFile(*serverFile)
	distributedGrep(serverMap, *pattern, *filePrefix, *terminal)

}