package main

import (
	"net"
	"os"
	"fmt"
	"time"
	"sync"
)
// Let's create the ping function , with timeout of 

var client [2]string = [2]string{"10.193.204.136","172.16.197.192"}
var validIP [2]bool = [2]bool{true,true}

var mutex = &sync.Mutex{}

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

func ping_client(){
	myIP := getmyIP()
	
	for{
		for i:=0; i< len(client);i++{
			clientIP = client[i]
			timeOut = time.Duration(2) * time.Second // TimeOut
			if clientIP == myIP {
				fmt.Printf("This is me")
				continue
			} else {
				conn, err := net.DialTimeout("tcp", clientIP+":8081",timeOut)
				if err != nil {
                 	// Set the client as dead using mutex
                 	fmt.Printf("Error in code")	
                 	
       	    	} else{

       	    		// mutex set to true
       	    		fmt.Printf("Server is alive !!")	
       	    		// done
       	    	}
			}

		}
		time.Sleep(2 * time.Second)
	}

	
}



func main() {

	resIp := getmyIP() // myIP  for test
	fmt.Printf("%s\n",resIp)
	fmt.Printf("%t\n",validIP[3])
	// ping_client()
	fmt.Printf("%t\n",validIP[3])
	go ping_client()
	return
}

