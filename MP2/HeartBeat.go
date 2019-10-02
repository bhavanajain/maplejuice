package main

import (
	"strings"
	"strconv"
	"os"
	"fmt"
	"time"
	"net"
)

var heartbeatPeriod = 2

type Node struct {
    IP string
    count  int64
    vid int
}



var heartbeatStatus = make(map[string] Node)
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

var myIP string

// HeartBeatCount = make(map[string]int)

var HeartNeigh = []string{"127.0.0.1","128.0.0.2"} 

func send_heartbeat() {

	for{

		for _, neighIP := range HeartNeigh{
			conn, err := net.Dial("udp", elem +":8080")
			// Is it possible to keep a map of monitorid -> UDP connection with it? 

			if err != nil {
				fmt.Println("[Error] Error in Heartbeat to  ", neighIP)
			} 
			msg_UDP := strconv.Itoa(time.Now().Unix())
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
        IP: net.ParseIP(myIP),    // need to setUp my IP
    }
    ser, err := net.ListenUDP("udp", &addr)
    if err != nil {
        fmt.Println("[Error] Listener setting up error %v", err)
        return
    }
    for {
        _,remoteaddr,err := ser.ReadFromUDP(p)
        fmt.Printf("Read a message from %v %s \n", remoteaddr, p)
        // Modify the map
        HeartBeatCount[remoteaddr] = time.Now().Unix()
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
		curr_time = time.Now().Unix()
		for k,v := range heartbeatStatus{
			if curr_time - v.count > 2*heartbeatPeriod {
				go check_suspiscion(v.vid, v.IP)
			}
		}
		time.Sleep(heartbeatPeriod * time.Second)
	}

}

