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
	// "github.com/gookit/color"
	"os/exec"
	"math/rand"
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

	outFile := "Out.txt"
	var err error

	var fout *os.File
	if terminal == false {
		fout,err = os.Create(outFile)
		if err != nil{
			fmt.Printf("Error in creating file")
		}

	}

	for serverIP, fileIdx := range(serverMap) {
		go patternMatch(serverIP, pattern, fileIdx, filePrefix, terminal, &wg, fout) // added fout
	}

	wg.Wait()
}

func patternMatch(serverIP string, pattern string, fileIdx int, filePrefix string, terminal bool, wg *sync.WaitGroup, foutput *os.File) {
	defer wg.Done()

	timeout := time.Duration(20) * time.Second
	filename := fmt.Sprintf("%s%d.log", filePrefix, fileIdx)

	conn, err := net.DialTimeout("tcp", serverIP + ":8080", timeout)
	if err != nil {
		fmt.Printf("Error connecting with client %s")
		return
	}
	defer conn.Close()

	// magenta := color.FgMagenta.Render
 //    bold := color.OpBold.Render

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

			split_line := strings.Split(line, "$$$$")
			linenum_str, line := split_line[0], split_line[1]
			linenum, _ := strconv.Atoi(linenum_str)

			mutex.Lock()
			if !terminal {
				fmt.Fprintf(foutput, "[%s] %d: ", filename, linenum)
			}else {
				fmt.Printf("[%s] %d: ", filename, linenum)
			}
			indices_grid := r.FindAllStringIndex(line, -1)
			next_start := 0
			for _, indices := range indices_grid {
				if !terminal {
					fmt.Fprintf(foutput,"%s%s", line[next_start:indices[0]], line[indices[0]:indices[1]])
				}else {
					fmt.Printf("%s%s", line[next_start:indices[0]], line[indices[0]:indices[1]])
				}
				
				next_start = indices[1]
			}

			if !terminal {
					fmt.Fprintf(foutput,"%s", line[next_start:])
					//fmt.Printf("%s", line[next_start:])
			}else{

					fmt.Printf("%s", line[next_start:])
			}
			// fmt.Printf("%s", line[next_start:])

			mutex.Unlock()
		}
		if (done) {
			break
		}
	}	
	return
}

// func init() {
//     rand.Seed(time.Now().UnixNano())
// }

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$^&*()")

func RandStringRunes(n int) string {
	// rand.Seed(time.Now().UnixNano())
    b := make([]rune, n)
    for i := range b {
        b[i] = letterRunes[rand.Intn(len(letterRunes))]
    }
    return string(b)
}

func Test1(serverMap map[string]int, pattern string, filePrefix string, terminal bool) {
	fmt.Printf("Inside Test")
	// numClient = len(serverMap)
	rand.Seed(time.Now().UnixNano())
	//var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$^&*()")


	// Creating files
	for serverIP, fileIdx := range(serverMap) {
		filename := fmt.Sprintf("testvm%d.log",fileIdx)
		f,err := os.Create(filename)
		if err != nil {
	        fmt.Println(err)
	        f.Close()
	        continue
   		}

   		for ln := 0;ln<20;ln++ {
   			outPut := RandStringRunes(20)+"\n"
   			fmt.Fprintf(f,outPut)
   		}
   		fmt.Fprintf(f,"abcdefA09856\n")
   		err = f.Close()

   		file_server(serverIP,filename)

  //  		outAddr := "dipayan2@"+serverIP+":/home/cs425/MP1"
  //  		fmt.Printf("Trying %s\n",string(outAddr))
		// cmdStr := "scp "+string(filename)+" "+string(outAddr)
  //  		_, err = exec.Command(string(cmdStr)).Output()

   		// for o := 1; o <= 10; o++ {
   		// 	command := fmt.Sprintf("")
   		// 	_, err = exec.Command("scp testvm." + strconv)

   		// }
   		fmt.Printf(serverIP)

   		// command := fmt.Sprintf("scp testvm.%d.log $vm01:/home/cs425/MP1", fileIdx)
   		// _, err = exec.Command(command).Output()


   		if err != nil {
   			fmt.Printf("%s \n",err)
   		}

   		// distributedGrep(serverMap,pattern,filePrefix,terminal)

   		// Match the outPut file

	//go patternMatch(serverIP, pattern, fileIdx, filePrefix, terminal, &wg)

	}

	distributedGrep(serverMap,pattern,filePrefix,terminal)
	 // Check the Output with set Patterns



	
}

const BUFFERSIZE = 1024

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

func sendFileToClient(connection net.Conn, filename string) {
	fmt.Println("A client has connected!")
	defer connection.Close()
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println(err)
		return
	}
	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Println(err)
		return
	}
	fileSize := fillString(strconv.FormatInt(fileInfo.Size(), 10), 10)
	fileName := fillString(fileInfo.Name(), 64)
	fmt.Println("Sending filename and filesize!")
	connection.Write([]byte(fileSize))
	connection.Write([]byte(fileName))
	sendBuffer := make([]byte, BUFFERSIZE)
	fmt.Println("Start sending file!")
	for {
		_, err = file.Read(sendBuffer)
		if err == io.EOF {
			break
		}
		connection.Write(sendBuffer)
	}
	fmt.Println("File has been sent, closing connection!")
	return
}


func file_server(serverIP string, fileName string) {
	//server, err := net.Listen("tcp", "localhost:27001")
	timeout := time.Duration(20) * time.Second
	connection, err := net.DialTimeout("tcp", serverIP + ":27001", timeout)
	// defer connection.Close()
	if err != nil {
		fmt.Println("Error listetning: ", err)
		//os.Exit(1)
		return
	}
	
	fmt.Println("Dialing started! Connected ready for sending...")
	sendFileToClient(connection,fileName)
	// for {
	// 	connection, err := server.Accept()
	// 	if err != nil {
	// 		fmt.Println("Error: ", err)
	// 		os.Exit(1)
	// 	}
	// 	fmt.Println("Client connected")
	// 	go sendFileToClient(connection)
	// }
}

func main(){
	serverFile := flag.String("server_file", "new_servers.in", "File containing the IP and idx for distributed machines")
	pattern := flag.String("pattern", "^[0-9]*[a-z]{5}", "regexp pattern to match in distributed files")
	filePrefix := flag.String("file_prefix", "testvm", "prefix of the file names on distributed machines")
	terminal := flag.Bool("terminal", false, "print output on terminal if true else store in separate files")

	flag.Parse()

	serverMap := parseServerFile(*serverFile)
	//distributedGrep(serverMap, *pattern, *filePrefix, *terminal)

	// Testing part
	// -- Create the log files and send to vms
	Test1(serverMap,*pattern,*filePrefix,*terminal)

	// file_server()

	

}
