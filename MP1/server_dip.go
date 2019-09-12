package main

import (
	"net"
	"fmt"
	"time"
	"sync"
	"strings"
	"bufio"
    "github.com/gookit/color"
    "regexp"
    "strconv"
    "bytes"
)

var client [2]string = [2]string{"10.193.204.136","172.16.197.192"}
var output [2]string = [2]string{"",""}
var validIP [2]bool = [2]bool{true,true}

var mutex = &sync.Mutex{}

func distributedGrep(pattern string){
	timeOut := time.Duration(10) * time.Second

	r, _ := regexp.Compile(pattern)

	magenta := color.FgMagenta.Render
    bold := color.OpBold.Render

    var wg sync.WaitGroup
    wg.Add(len(client))

	for i:=0; i<len(client); i++ {
		go pattern_match_thread(&wg,i,pattern)

	}

	wg.Wait()
	fmt.Println("Write the Output")

	for i:=0; i< len(client); i++{

			if len(output[i]) > 0 {
				filename := fmt.Sprintf("machine.%d.log", i)
				fmt.Println(filename)
				var idx_arr [][] int
				out_string := strings.Split(output[i],"\n")

				for rbuf := 0; rbuf < len(out_string); rbuf ++{
					split_results := strings.Split(out_string[rbuf], ":")
					linenum_str, line := split_results[0], split_results[1]
					linenum, _ := strconv.Atoi(linenum_str)
					fmt.Printf("%d:", linenum)
					idx_arr = r.FindAllStringIndex(line, -1)
					var itest int = 0
            		for _, element := range idx_arr {
                		fmt.Printf("%s%s", line[itest:element[0]], bold(magenta(line[element[0]:element[1]])))
                		itest = element[1]
            		}
           			fmt.Printf("%s", line[itest:])

				}


			}
	}

	fmt.Println("I'm Done")

}


func pattern_match_thread(wg *sync.WaitGroup, i int, pattern string) {
	defer wg.Done()

	mutex.Lock()
	isAlive := validIP[i]
	mutex.Unlock()

	if isAlive {
		conn, err := net.DialTimeout("tcp", client[i] + ":8080", timeOut)
		if err != nil {
			fmt.Printf("Error connecting with client %d: %s", i, client[i])
			mutex.Lock()
			validIP[i] = false
			mutex.Unlock()
			output[i] = ""
			return
		}
		buf := bytes.Buffer{}
		for {

			// filename := fmt.Sprintf("machine.%d.log", i)
			// fmt.Println(filename)
			parameters := filename + "," + pattern + "\n"
			fmt.Fprintf(conn, parameters)

			reader := bufio.NewReader(conn)
			var done bool = false
			// var idx_arr [][]int
			for {
				results,_ := reader.ReadString('\n')
				if strings.Contains(results, "<EOF>"){
					done = true
					break
				}
				// Store the result in some buffer
				buf.WriteString(results)

				// This part will be done after reading the results, or we can do this later
				// split_results := strings.Split(results, ":")
				// linenum_str, line := split_results[0], split_results[1]
				// linenum, _ := strconv.Atoi(linenum_str)

				// fmt.Printf("%d:", linenum)
				// idx_arr = r.FindAllStringIndex(line, -1)
				// var i int = 0
    //     		for _, element := range idx_arr {
    //         		fmt.Printf("%s%s", line[i:element[0]], bold(magenta(line[element[0]:element[1]])))
    //         		i = element[1]
    //     		}
    //    			fmt.Printf("%s", line[i:])

			}
			if done {
				// Write the buffer in a global string
				output[i] = ""
				output[i] = buf.String()
				break
			}
			//color part will be handled by the master
		}
		conn.Close()
	} 

	return
	
}





func main() {
	distributedGrep("te")
}

