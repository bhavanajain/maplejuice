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
	"math/rand"
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

func distributedGrep(serverMap map[string]int, pattern string, filePrefix string, visual bool, fout *os.File) {
	var wg sync.WaitGroup
	num_servers := len(serverMap)
	wg.Add(num_servers)

	for serverIP, fileIdx := range(serverMap) {
		go patternMatch(serverIP, pattern, fileIdx, filePrefix, visual, fout, &wg)
	}

	wg.Wait()
}

func patternMatch(serverIP string, pattern string, fileIdx int, filePrefix string, visual bool, fout *os.File, wg *sync.WaitGroup) {
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
			mutex.Lock()
			fmt.Fprintf(fout, "[%s] %d: %s", filename, linenum, line)
			mutex.Unlock()
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

func CheckOutput(expectedname string, generatedname string) bool {
	expected, err := os.Open(expectedname)
	if err != nil {
		fmt.Printf("[Output Checker] Can't open file %s\n", expectedname)
		return false
	}

	reader1 := bufio.NewReader(expected)
	var expectedlines []string

	for {
		line, err := reader1.ReadString('\n')
		if len(line) > 0 {
			expectedlines = append(expectedlines, line[:len(line)-1])
		}
		if err != nil {
			if err != io.EOF {
				fmt.Printf("[File Checker] Unknown error while reading file %s", expectedname)
				return false
			}
			break
		}
	}

	matchedlines := make([]bool, len(expectedlines))
	for i:=0; i<len(expectedlines); i++ {
		matchedlines[i] = false
	}

	generated, err := os.Open(generatedname)
	if err != nil {
		fmt.Printf("[Output Checker] Can't open file %s\n", generatedname)
		return false
	}

	reader2 := bufio.NewReader(generated)
	for {
		line, err := reader2.ReadString('\n')
		if len(line) > 0 {
			for j:=0; j<len(expectedlines); j++ {
				if line[:len(line)-1] == expectedlines[j] {
					matchedlines[j] = true
				}
			}
		}
		if err != nil {
			if err != io.EOF {
				fmt.Printf("[File Checker] Unknown error while reading file %s", expectedname)
				return false
			}
			break
		}
	}

	for k:=0; k<len(matchedlines); k++ {
		if !matchedlines[k] {
			return false
		}
	}

	return true
}



func main(){
	serverFile := flag.String("server_file", "servers.in", "path to the file containing server IPs and index")
	pattern := flag.String("pattern", "^[0-9]*[a-z]{5}", "regexp pattern to match")
	filePrefix := flag.String("file_prefix", "vm", "prefix of the files before <i>.log")
	visual := flag.Bool("visual", false, "boolean flag, when set, prints annotated matches to the terminal and highlights patterns.	when false, the each server output is stored separately in 'filtered-<file_prefix><i>.log'")
	
	flag.Parse()
	fmt.Printf("Dummy print: %s", *pattern)

	serverMap := parseServerFile(*serverFile)
	for ip, idx := range(serverMap) {
		fmt.Println(ip, idx)
	}

	rand.Seed(time.Now().UnixNano())

	expectedOutput_freq, err := os.Create("expected_freq.out")
	if err != nil {
		fmt.Printf("[Test Client] Couldn't create expected frequent file\n")
		return
	}
	expectedOutput_mod, err := os.Create("expected_mod.out")
	if err != nil {
		fmt.Printf("[Test Client] Couldn't create expected moderate file\n")
		return
	}
	expectedOutput_inf, err := os.Create("expected_inf.out")
	if err != nil {
		fmt.Printf("[Test Client] Couldn't create expected infrequent file\n")
		return
	}

	frequent := "abcde"
	infrequent := "qwerty"
	moderate := "12345"

	for serverIP, fileIdx := range(serverMap) {
		filename := fmt.Sprintf("testvm%d.log", fileIdx)
		f, err := os.Create(filename)
		if err != nil {
			fmt.Printf("[Test Client] Couldn't generate file for %s %d\n", serverIP, fileIdx)
			return
		}

		for ln:=0; ln<1000; ln++ {
			val := rand.Intn(1000)
			var output string
			if val < 700 {
				output = frequent + "\n"
				fmt.Fprintf(expectedOutput_freq,"[%s] %d: %s", filename, ln+1, output)
			} else if val < 900{
				output = moderate + "\n"
				fmt.Fprintf(expectedOutput_mod,"[%s] %d: %s", filename, ln+1, output)
			} else{
				output = infrequent + "\n"
				fmt.Fprintf(expectedOutput_inf,"[%s] %d: %s", filename, ln+1, output)
			}
   			fmt.Fprintf(f, output)
   		}
   		
   		fmt.Printf("[Test Client] Created file for %s %d\n", serverIP, fileIdx)
   		f.Close()

   		SendFile(serverIP, filename)
	}

	expectedOutput_freq.Close()
	expectedOutput_mod.Close()
	expectedOutput_inf.Close()

	freq_results, _ := os.Create("freq_results.out")
	mod_results, _ := os.Create("mod_results.out")
	inf_results, _ := os.Create("inf_results.out")

	distributedGrep(serverMap, frequent, *filePrefix, *visual, freq_results)
	distributedGrep(serverMap, moderate, *filePrefix, *visual, mod_results)
	distributedGrep(serverMap, infrequent, *filePrefix, *visual, inf_results)

	if CheckOutput("freq_results.out", "expected_freq.out") {
		fmt.Printf("[Test Passed] Frequent pattern matched\n")
	} else {
		fmt.Printf("[Test Failed] Frequent pattern does not match\n")
	}

	if CheckOutput("inf_results.out", "expected_inf.out") {
		fmt.Printf("[Test Passed] Infrequent pattern matched\n")
	} else {
		fmt.Printf("[Test Failed] Infrequent pattern does not match\n")
	}

	if CheckOutput("mod_results.out", "expected_mod.out") {
		fmt.Printf("[Test Passed] Moderate pattern matched\n")
	} else {
		fmt.Printf("[Test Failed] Moderate pattern does not match\n")
	}	

}
