package main

import (
	"bufio"
	"strings"
	"strconv"
	"flag"
	"os"
	"fmt"
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
		fmt.Println("[Error] Couldn't open the file", serverFile)
		return serverMap
	}

	reader := bufio.NewReader(file)
	var line string
	for {
		line, err = reader.ReadString('\n')
		if len(line) > 0 {
			if line[len(line)-1] != '\n' {
				line = line + "\n"
			}
			line = line[:len(line)-1]
			split_line := strings.Split(line, " ")
			serverMap[split_line[0]], _ = strconv.Atoi(split_line[1])
		}		

		if err != nil {
			if err != io.EOF {
				fmt.Printf("[Error] Unknown error while reading file", serverFile)
			}
			break
		}		
	}

	return serverMap
}

func distributedGrep(serverMap map[string]int, pattern string, filePrefix string, visual bool) {
	var wg sync.WaitGroup
	num_servers := len(serverMap)
	wg.Add(num_servers)

	for serverIP, fileIdx := range(serverMap) {
		go patternMatch(serverIP, pattern, fileIdx, filePrefix, visual, &wg)
	}

	wg.Wait()
}

func patternMatch(serverIP string, pattern string, fileIdx int, filePrefix string, visual bool, wg *sync.WaitGroup) {
	defer wg.Done()

	timeout := time.Duration(20) * time.Second
	filename := fmt.Sprintf("%s%d.log", filePrefix, fileIdx)

	conn, err := net.DialTimeout("tcp", serverIP + ":8080", timeout)
	defer conn.Close()
	if err != nil {
		fmt.Printf("[Error] Unable to connect with the client %s", serverIP)
		return
	}

    r, _ := regexp.Compile(pattern)

	parameters := filename + "," + pattern + "\n"
	fmt.Fprintf(conn, parameters)

	w := bufio.NewWriter(os.Stdout)

	magenta := color.FgMagenta.Render
    bold := color.OpBold.Render

	if visual {
    	w = bufio.NewWriter(os.Stdout)
    } else {
    	outfile_name := fmt.Sprintf("filtered-%s%d.out", filePrefix, fileIdx)
    	outfile, _ := os.Create(outfile_name)
    	w = bufio.NewWriter(outfile)
    }

	reader := bufio.NewReader(conn)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("[Error] Server %s has crashed", serverIP)
			break
		}
		if strings.Contains(line, "<<EOF>>") {
			closing_list := strings.Split(line, ",")
			num_matches, _ := strconv.Atoi(closing_list[0])
			fmt.Fprintf(w, "[%s] Line count: %s", filename, num_matches)
			break
		}

		split_line := strings.Split(line, "$$$$")
		linenum_str, line := split_line[0], split_line[1]
		linenum, _ := strconv.Atoi(linenum_str)

		if (visual) {
			mutex.Lock()
			fmt.Fprintf(w, "[%s] %d: ", filename, linenum)
			indices_grid := r.FindAllStringIndex(line, -1)
			next_start := 0
			for _, indices := range indices_grid {
				fmt.Fprintf(w, "%s%s", line[next_start:indices[0]], bold(magenta(line[indices[0]:indices[1]])))
				next_start = indices[1]
			}
			fmt.Fprintf(w, "%s", line[next_start:])
			mutex.Unlock()
		} else {
			fmt.Fprintf(w, "[%s] %d: %s", filename, linenum, line)
		}
	}

	return
}

func main(){
	serverFile := flag.String("server_file", "servers.in", "File containing the IP and idx for distributed machines")
	pattern := flag.String("pattern", "^[0-9]*[a-z]{5}", "regexp pattern to match in distributed files")
	filePrefix := flag.String("file_prefix", "vm", "prefix of the file names on distributed machines")
	visual := flag.Bool("visual", true, "highlight the matched instances of the pattern, install the color package using `go get github.com/gookit/color'")
	
	flag.Parse()

	serverMap := parseServerFile(*serverFile)
	distributedGrep(serverMap, *pattern, *filePrefix, *visual)
}