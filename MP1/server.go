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
)

var client [2]string = [2]string{"10.193.204.136","172.16.197.192"}
var validIP [2]bool = [2]bool{true,true}

var mutex = &sync.Mutex{}

func distributedGrep(pattern string){
	timeOut := time.Duration(10) * time.Second

	r, _ := regexp.Compile(pattern)

	magenta := color.FgMagenta.Render
    bold := color.OpBold.Render

	for i:=0; i<len(client); i++ {
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
				continue
			}
			for {

				filename := fmt.Sprintf("machine.%d.log", i)
				fmt.Println(filename)
				parameters := filename + "," + pattern + "\n"
				fmt.Fprintf(conn, parameters)

				reader := bufio.NewReader(conn)
				var done bool = false
				var idx_arr [][]int
				for {
					results,_ := reader.ReadString('\n')
					if strings.Contains(results, "<EOF>"){
						done = true
						break
					}
					split_results := strings.Split(results, ":")
					linenum_str, line := split_results[0], split_results[1]
					linenum, _ := strconv.Atoi(linenum_str)

					fmt.Printf("%d:", linenum)
					idx_arr = r.FindAllStringIndex(line, -1)
					var i int = 0
            		for _, element := range idx_arr {
                		fmt.Printf("%s%s", line[i:element[0]], bold(magenta(line[element[0]:element[1]])))
                		i = element[1]
            		}
           			fmt.Printf("%s", line[i:])

				}
				if done {
					break
				}
				//color part will be handled by the master
			}
			conn.Close()
		}

	}

}





func main() {
	distributedGrep("te")
}

