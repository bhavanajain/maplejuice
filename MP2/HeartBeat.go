package main

import (
	"strconv"
	"fmt"
	"time"
	"net"
	"os"
	"sync"
)

var introducer = "172.22.152.106:8000"

type Node struct {
	virt_id int
	ip string
	timestamp int64
	conn     *net.UDPConn
}

var members = []Node

func join(){
	for {
		conn, err := net.Dial("udp", introducer)
		if err != nil {
			fmt.Println("[Join] Unable to send JOIN request to the introducer %s", introducer)
		}
		defer conn.Close()
		dummy := "JOIN"
		fmt.Fprintf(conn, dummy)
		// TODO: receive an ack, break out of the loop
	}
	return
}

type Message struct {
	event string // JOIN, LEAVE, CRASH
	userId string
}

func disseminate(m Message){
	msgString = packMessage(m)
	for i, node := members {
		conn, err := net.Dial("udp", node.ip + port)
		if err != nil {
			fmt.Println("[Error] Cannot disseminate to ", node.ip)
		}
		defer conn.Close()
		fmt.Fprintf(conn, msgString)
		// todo: udp packets may be dropped
	}
}

func leave(){
	for i, node := range(members) {
		conn, err := net.Dial("udp", neighIP +":8080")
			// Is it possible to keep a map of monitorid -> UDP connection with it? 

			if err != nil {
				fmt.Println("[Error] Error in Heartbeat to  ", neighIP)
			} 
			msg_UDP := strconv.FormatInt(time.Now().Unix(),10)
			fmt.Fprintf(conn, msg_UDP)
			conn.Close()



	}
}











// var heartbeatPeriod = 2

// type Node struct {
//     IP string
//     count  int64
//     vid int
// }



// var heartbeatStatus = make(map[string] Node)
// monitorsConnMap := make(map[string]net.Conn)

// func setupMonitorConns([]string monitors) {
// 	for monitor in monitors {
// 		conn, err := net.Dial("udp", monitor + ":8080")
// 		if err != nil {
// 			fmt.Println("Could not setup connection with ", monitor)
// 			continue
// 		}
// 		monitorsConnMap[monitor] = conn
// 	}
// }

// func sendHeartBeats() {
// 	for {
// 		for monitor, conn in monitorsConnMap.items() {
// 			fmt.Fprintf(conn, message)
// 		}
// 		time.Sleep(heartbeatPeriod * time.Second)
// 	}
// }

// func receive


// Note how to get the count, still thinking, can use time.Now().Unix()

// var myIP string

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

var HeartBeatCount = make(map[string]int64)

var HeartNeigh = []string{"172.22.152.108","172.22.156.103"} 

func send_heartbeat() {

	for{

		for _, neighIP := range HeartNeigh{
			conn, err := net.Dial("udp", neighIP +":8080")
			// Is it possible to keep a map of monitorid -> UDP connection with it? 

			if err != nil {
				fmt.Println("[Error] Error in Heartbeat to  ", neighIP)
			} 
			msg_UDP := strconv.FormatInt(time.Now().Unix(),10)
			fmt.Fprintf(conn, msg_UDP)
			conn.Close()
		}

		time.Sleep(1 * time.Second) // Sleep 1 second beofre re- starting the heartbeat messages
	}


	return

}

func recv_heartbeat() {

	p := make([]byte, 2048)
    addr := net.UDPAddr{
        Port: 8080,
        IP: net.ParseIP(getmyIP()),    // need to setUp my IP
    }
    ser, err := net.ListenUDP("udp", &addr)
    if err != nil {
        fmt.Println("[Error] Listener setting up error %v", err)
        return
    }
    for {
    	fmt.Println("[Log] Waiting for connection heartbeat")
        _,remoteaddr,err := ser.ReadFromUDP(p)
        fmt.Printf("Read a message from %v %s \n", remoteaddr, p)
        // Modify the map
        HeartBeatCount[remoteaddr.String()] = time.Now().Unix()
        if err !=  nil {
            fmt.Printf("Some error  %v", err)
            continue
        }

    }
	
}

func check_suspiscion(virt_id int, node_IP string) {

	
}

func track_targets() {

	for{
		curr_time := time.Now().Unix()
		for k,v := range HeartBeatCount{
			if curr_time - v > 4 {
				fmt.Println("[Log] Node %s is in trouble",k)
				// go check_suspiscion(v.vid, v.IP)
			}
		}
		time.Sleep(2 * time.Second)
	}

}

func main() {
	var wg sync.WaitGroup
	wg.Add(2)
	go recv_heartbeat()
	go track_targets()
	wg.Wait()
	return

	
}

