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

// func parseServerFile(serverFile string) map[string]int {
// 	serverMap := make(map[string]int)

// 	file, err := os.Open(serverFile)
// 	if err != nil {
// 		fmt.Println("[Error] Couldn't open the file", serverFile)
// 		return serverMap
// 	}
// 	defer file.Close()


// 	reader := bufio.NewReader(file)
// 	var line string
// 	for {
// 		line, err = reader.ReadString('\n')
// 		if len(line) > 0 {
// 			if line[len(line)-1] != '\n' {
// 				line = line + "\n"
// 			}
// 			line = line[:len(line)-1]
// 			split_line := strings.Split(line, " ")
// 			serverMap[split_line[0]], _ = strconv.Atoi(split_line[1])
// 		}		

// 		if err != nil {
// 			if err != io.EOF {
// 				fmt.Println("[Error] Unknown error while reading file", serverFile)
// 			}
// 			break
// 		}		
// 	}

// 	return serverMap
// }

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
	fout.Close()
}

// func distributedGrep(serverMap map[string]int, pattern string, filePrefix string, visual bool) {
// 	var wg sync.WaitGroup
// 	num_servers := len(serverMap)
// 	wg.Add(num_servers)

// 	for serverIP, fileIdx := range(serverMap) {
// 		go patternMatch(serverIP, pattern, fileIdx, filePrefix, visual, &wg)
// 	}

// 	wg.Wait()
// }
func patternMatch(serverIP string, pattern string, fileIdx int, filePrefix string, visual bool, wg *sync.WaitGroup,foutput *os.File) {
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
			fmt.Fprintf(foutput, "[%s] %d: %s",filename,linenum, line)
			mutex.Unlock()
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


func checkOutPut(file1 string, file2 string) bool {

	fp1, err := os.Open(file2)

	if err != nil {
		fmt.Printf("Error opening file")
	}

	file_reader := bufio.NewReader(fp1)

	var toCheckFile []string

	for {
		line , err := file_reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				fmt.Printf("Error Reading file !!")
			}
			break
		}
		toCheckFile = append(toCheckFile, line[:len(line)-1])
	}

	checkTest := make([]bool, len(toCheckFile))
	for i:= 0 ; i< len(toCheckFile); i++{
		checkTest[i] = false
	}

	fp2, err := os.Open(file1)

	if err != nil {
		fmt.Printf("Error opening file")
	}

	file_readerN := bufio.NewReader(fp2)

	for{
		line, err := file_readerN.ReadString('\n')
		if err != nil{
			if err != io.EOF {
				fmt.Printf("Error reading Output")
			}
			break
		}

		for j:= 0; j< len(toCheckFile); j++{
			if line[:len(line)-1] == toCheckFile[j] {
				checkTest[j] = true
			}
		}
	}

	flag := 1

	for j:= 0; j< len(checkTest); j++{
		if checkTest[j] == false{
			flag = 0
			break
		}
	}

	if flag == 1 {
		return true
	}

	return false



	
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
	
	fmt.Println("\nDialing started! Connected ready for sending...")
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


func Test1(serverMap map[string]int, pattern string, filePrefix string, terminal bool, testFile string) {
	fmt.Println("\nInside  Basic Dist Test \n")
	// numClient = len(serverMap)
	rand.Seed(time.Now().UnixNano())
	//var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$^&*()")
	sample_out,err := os.Create(testFile)
	if err != nil {
        fmt.Println("Sample File Not created!!")
        sample_out.Close()
	}

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
   		for ln := 20; ln<21;ln++{
   			textStr := "abcdefA09856\n"
   			fmt.Fprintf(f,textStr)
   			fmt.Fprintf(sample_out,"[%s] %d: %s",filename,ln+1,textStr)
   		}

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
   		// fmt.Printf(serverIP)

   		// command := fmt.Sprintf("scp testvm.%d.log $vm01:/home/cs425/MP1", fileIdx)
   		// _, err = exec.Command(command).Output()


   		if err != nil {
   			fmt.Printf("%s \n",err)
   		}

   		// distributedGrep(serverMap,pattern,filePrefix,terminal)

   		// Match the outPut file

	//go patternMatch(serverIP, pattern, fileIdx, filePrefix, terminal, &wg)

	}

	sample_out.Close()

	fmt.Printf("Started Dist Grep")

	distributedGrep(serverMap,pattern,filePrefix,terminal)
	 // Check the Output with set Patterns
	outVal := checkOutPut("Out.txt",testFile)

	if outVal {
		fmt.Printf("\nCorrect outPut!!")
	} else{
		fmt.Printf("\n Error !!")
	}



	
}


func Test2(serverMap map[string]int, pattern string, filePrefix string, terminal bool, testFile string) {
	fmt.Println("\nInside Incorrect Check Test \n")
	// numClient = len(serverMap)
	rand.Seed(time.Now().UnixNano())
	//var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$^&*()")
	sample_out,err := os.Create(testFile)
	if err != nil {
	        fmt.Println("Sample File Not created!!")
	        sample_out.Close()
   		}

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

   		if fileIdx == 10{
   			for ln := 20; ln<1000;ln++{
   			textStr := "abcdefB09856\n"
   			textStr1 := "abcdefA09856\n"
   			fmt.Fprintf(f,textStr)
   			fmt.Fprintf(sample_out,"[%s] %d: %s",filename,ln+1,textStr1)
   			}

   		} else {
	   		for ln := 20; ln<1000;ln++{
	   			textStr := "abcdefA09856\n"
	   			fmt.Fprintf(f,textStr)
	   			fmt.Fprintf(sample_out,"[%s] %d: %s",filename,ln+1,textStr)
	   		}
   		}

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
   		// fmt.Printf(serverIP)

   		// command := fmt.Sprintf("scp testvm.%d.log $vm01:/home/cs425/MP1", fileIdx)
   		// _, err = exec.Command(command).Output()


   		if err != nil {
   			fmt.Printf("%s \n",err)
   		}

   		// distributedGrep(serverMap,pattern,filePrefix,terminal)

   		// Match the outPut file

	//go patternMatch(serverIP, pattern, fileIdx, filePrefix, terminal, &wg)

	}

	sample_out.Close()

	fmt.Printf("Started Dist Grep")

	distributedGrep(serverMap,pattern,filePrefix,terminal)
	 // Check the Output with set Patterns
	outVal := checkOutPut("Out.txt",testFile)

	if outVal {
		fmt.Printf("\nCorrect outPut!!")
	} else{
		fmt.Printf("\n Error !!")
	}



	
}



func Test3(serverMap map[string]int, pattern string, filePrefix string, terminal bool, testFile string) {
	fmt.Println("\nInside Frequent Pattern File Dist Test \n")
	// numClient = len(serverMap)
	rand.Seed(time.Now().UnixNano())
	//var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$^&*()")
	sample_out,err := os.Create(testFile)
	if err != nil {
	        fmt.Println("Sample File Not created!!")
	        sample_out.Close()
   		}

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
   		for ln := 20; ln<1000;ln++{
   			textStr := "abcdefA09856\n"
   			fmt.Fprintf(f,textStr)
   			fmt.Fprintf(sample_out,"[%s] %d: %s",filename,ln+1,textStr)
   		}

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
   		// fmt.Printf(serverIP)

   		// command := fmt.Sprintf("scp testvm.%d.log $vm01:/home/cs425/MP1", fileIdx)
   		// _, err = exec.Command(command).Output()


   		if err != nil {
   			fmt.Printf("%s \n",err)
   		}

   		// distributedGrep(serverMap,pattern,filePrefix,terminal)

   		// Match the outPut file

	//go patternMatch(serverIP, pattern, fileIdx, filePrefix, terminal, &wg)

	}

	sample_out.Close()

	fmt.Printf("Started Dist Grep")

	distributedGrep(serverMap,pattern,filePrefix,terminal)
	 // Check the Output with set Patterns
	outVal := checkOutPut("Out.txt",testFile)

	if outVal {
		fmt.Printf("\nCorrect outPut!!")
	} else{
		fmt.Printf("\n Error !!")
	}



	
}


func main(){
	serverFile := flag.String("server_file", "server_new.in", "File containing the IP and idx for distributed machines")
	pattern := flag.String("pattern", "^[0-9]*[a-z]{5}", "regexp pattern to match in distributed files")
	filePrefix := flag.String("file_prefix", "testvm", "prefix of the file names on distributed machines")
	visual := flag.Bool("visual", false, "highlight the matched instances of the pattern, install the color package using `go get github.com/gookit/color'")
	
	flag.Parse()

	serverMap := parseServerFile(*serverFile)
	fmt.Printf("Printing serverMap\n")
	for ip, idx := range(serverMap) {
		fmt.Println(ip, idx)
	}

	//distributedGrep(serverMap, *pattern, *filePrefix, *terminal)

	// Testing part
	// -- Create the log files and send to vms
	Test1(serverMap,*pattern,*filePrefix,*visual,"SampleOut1.txt")
	// Test1(serverMap,*pattern,*filePrefix,*visual,"SampleOut2.txt")
	// Test1(serverMap,*pattern,*filePrefix,*visual,"SampleOut3.txt")

	// file_server()

	

}
