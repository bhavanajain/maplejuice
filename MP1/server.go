package main

import (
	"net"
	"fmt"
	"time"
	"sync"
	"strings"
	"bufio"
)

var client [2]string = [2]string{"10.193.204.136","172.16.197.192"}
var validIP [2]bool = [2]bool{true,true}

var mutex = &sync.Mutex{}

func distributedGrep(pattern string){
	timeOut := time.Duration(10) * time.Second // TimeOut

	// r, _ := regexp.Compile(pattern)

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
				for {
					results,_ := reader.ReadString('\n')
					if strings.Contains(results, "<EOF>"){
						done = true
						break
					} 
					fmt.Printf("%s", results)
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
	distributedGrep("")
}

