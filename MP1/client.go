package main

import (
	"net"
	"os"
	"fmt"
)

// Let's create the ping function , with timeout of 

var client [2]string = [2]string{"10.193.204.136","172.16.197.192"}
var validIP [2]bool = [2]bool{true,true}

func getmyIP() string {
	addrs, err := net.InterfaceAddrs()
	var myip string = "-1"
	if err != nil {
		os.Stderr.WriteString("Oops: " + err.Error() + "\n")
		myip = "-1"
	}

	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				myip = ipnet.IP.String()
			}
		}
	}
	return myip
}
func heartbeat_check(){
	
	// This receives a connection from a server and replies whether it is alive or not
	ln, _ := net.Listen("tcp", ":8081")
	for {
		conn, _ := ln.Accept()
		conn.Close()
	}

	
}

func main() {

	 resIp := getmyIP() // myIP  for test
	 fmt.Printf("%s\n",resIp)
	 fmt.Printf("%t\n",validIP[3])
	 fmt.Printf("%t\n",validIP[3])
	 go heartbeat_check()
	
}

