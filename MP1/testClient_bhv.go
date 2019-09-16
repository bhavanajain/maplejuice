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
const BUFFERSIZE = 1024

func parseServerFile(serverFile string) map[string]int {
	serverMap := make(map[string]int)

	file, err := os.Open(serverFile)
	if err != nil {
		fmt.Println("[Error] Couldn't open the file", serverFile)
		return serverMap
	}
	defer file.Close()

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
				fmt.Println("[Error] Unknown error while reading file", serverFile)
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
	if err != nil {
		fmt.Println("[Error] Unable to connect with the client", serverIP)
		return
	}
	defer conn.Close()

    r, _ := regexp.Compile(pattern)

	parameters := filename + "," + pattern + "\n"
	fmt.Fprintf(conn, parameters)

	var w *bufio.Writer
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
			fmt.Printf("[Error] Server %s has crashed\n", serverIP)
			break
		}
		if strings.Contains(line, "<<EOF>>") {
			closing_list := strings.Split(line, ",")
			num_matches, _ := strconv.Atoi(closing_list[0])
			fmt.Printf("[%s] Line count: %d\n", filename, num_matches)
			fmt.Fprintf(w, "[%s] Line count: %d\n", filename, num_matches)
			w.Flush()
			break
		}

		split_line := strings.Split(line, "$$$$")
		linenum_str, line := split_line[0], split_line[1]
		linenum, _ := strconv.Atoi(linenum_str)

		if (visual) {
			magenta := color.FgMagenta.Render
    		bold := color.OpBold.Render
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
			fmt.Fprintln(w, "%d: %s", linenum, line[:len(line)-1])
		}
	}

	return
}

func fillString(retunString string, toLength int) string {
	for {
		lengtString := len(retunString)
		if lengtString < toLength {
			retunString = retunString + ":"
			continue
		}
		break
	}
	return retunString
}

func SendFile(serverIP string, filename string) {
	timeout := time.Duration(20) * time.Second

	conn, err := net.DialTimeout("tcp", serverIP + ":8081", timeout)
	if err != nil {
		fmt.Printf("[File Sender] Unable to connect with %s %s\n", serverIP, filename)
		return
	}
	defer conn.Close()

	f, err := os.Open(filename)
	if err != nil {
		fmt.Printf("[File Sender]: Abba, can't open file\n")
		return
	}

	fileInfo, err := f.Stat()
	if err != nil {
		fmt.Printf("[File Sender] Can't access file stats %s %s\n", serverIP, filename)
		return
	}

	fileSize := fillString(strconv.FormatInt(fileInfo.Size(), 10), 10)
	fileName := fillString(fileInfo.Name(), 64)

	fmt.Printf("[File Sender] filesize %s filename %s", fileSize, fileName)

	conn.Write([]byte(fileSize))
	conn.Write([]byte(fileName))

	sendBuffer := make([]byte, BUFFERSIZE)

	for {
		_, err = f.Read(sendBuffer)
		if err == io.EOF {
			break
		}
		conn.Write(sendBuffer)
	}
	fmt.Printf("[File Sender] Completed sending the file %s %s\n", serverIP, filename)

	return

}

func main(){
	serverFile := flag.String("server_file", "servers.in", "path to the file containing server IPs and index")
	pattern := flag.String("pattern", "^[0-9]*[a-z]{5}", "regexp pattern to match")
	filePrefix := flag.String("file_prefix", "vm", "prefix of the files before <i>.log")
	visual := flag.Bool("visual", false, "boolean flag, when set, prints annotated matches to the terminal and highlights patterns.	when false, the each server output is stored separately in 'filtered-<file_prefix><i>.log'")
	
	flag.Parse()

	serverMap := parseServerFile(*serverFile)
	for ip, idx := range(serverMap) {
		fmt.Println(ip, idx)
	}

	rand.Seed(time.Now().UnixNano())

	expectedOutput, err := os.Create('expected.out')
	if err != nil {
		fmt.Printf("[Test Client] Couldn't create expected file\n")
		return
	}

	for serverIP, fileIdx := range(serverMap) {
		filename := fmt.Sprintf("testvm%d.log", fileIdx)
		f, err := os.Create(filename)
		if err != nil {
			fmt.Printf("[Test Client] Couldn't generate file for %s %d\n", serverIP, fileIdx)
			return
		}

		for ln:=0; ln<20; ln++ {
   			output := "7365645723827856" + "\n"
   			fmt.Fprintf(f, output)
   		}
   		for ln:=20; ln<21; ln++{
   			textStr := "abcdefA09856\n"
   			fmt.Fprintf(f, textStr)
   			fmt.Fprintf(expectedOutput,"[%s] %d: %s", filename, ln+1, textStr)
   		}
   		fmt.Printf("[Test Client] Created file for %s %d\n", serverIP, fileIdx)
   		f.Close()

   		SendFile(serverIP, filename)
	}

	expectedOutput.Close()

	distributedGrep(serverMap, *pattern, *filePrefix, *visual)
}
