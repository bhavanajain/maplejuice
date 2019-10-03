package main

import (
	"strconv"
	"fmt"
	"time"
	"net"
	"os"
	"sync"
	"github.com/golang/glog"
	"strings"
)


type MemberNode struct {
	ip string
	timestamp int64
	alive bool
}

type MonitorNode struct {
	conn *net.UDPConn
	ip string
	vid int
}

type ChildNode struct {
	timestamp int64
	vid int
}

// todo
// type Message struct {
// 	message_type 
// }

var monitors [3]MonitorNode
var memberMap = make(map[int]*MemberNode)
var heartbeatPort = 8080
var myVid int	// this will be populated after join by introducer
var heartbeatPeriod int64 = 2	// in seconds
var children = make(map[string]*ChildNode)
var suspects []int 	// remove from suspects when leave or crash 
var otherPort = 8081
var delimiter = ","
var myIP string
// var introducer = "172.22.152.106"
var introducer = "172.22.154.104"
var introducerPort = 8082
// var otherportconn *net.UDPConn
// var heartbeatconn *net.UDPConn

func getMonitor(vid int) (node1 MonitorNode, err error) {
	var node MonitorNode
	node.vid = vid
	node.ip = memberMap[vid].ip

	var addr net.UDPAddr
	addr.IP = net.ParseIP(node.ip)
	addr.Port = heartbeatPort

	node.conn, err = net.DialUDP("udp", nil, &addr)
	if err != nil {
		glog.Warning("Cannot setup a UDP connection with monitor vid=%d ip=%s", node.vid, node.ip)
		return node, err
	}
	return node, nil
}

func setupMonitors() {
	n := len(memberMap)
	pred := (myVid - 1) % n
	succ1 := (myVid + 1) % n
	succ2 := (myVid + 2) % n

	for i, vid := range([]int{pred, succ1, succ2}) {
		node, err := getMonitor(vid)
		if err != nil {
			glog.Warning("Cannot setup a UDP connection with monitor vid=%d", vid)
		} else {
			monitors[i] = node
		}
	}
	return
}

// func setupListenHeartbeat() (err error) {
// 	udpAddress, err := net.ResolveUDPAddr("udp", heartbeatPort)
// 	if err != nil {
// 		glog.Warning("Unable to resolve my UDP address")
// 		return err
// 	}
// 	heartbeatconn, err = net.ListenUDP("udp", udpAddress)
// 	if err != nil {
// 		glog.Error("Unable to listen on the heartbeat port %s", heartbeatPort)
// 		return err
// 	}
// 	return
// }

func sendHeartbeat() {
	for {
		for _, node := range(monitors) {
			// dummy = strconv.FormatInt(time.Now().Unix(), 10)
			_, err := node.conn.Write([]byte("1"))
			if err != nil {
				glog.Warning("Could not send heartbeat myip=%s myvid=%d", myIP, myVid)
			}
		}
		time.Sleep(time.Duration(heartbeatPeriod) * time.Second)
	}
}

func receiveHeartbeat() {

	udpAddress, err := net.ResolveUDPAddr("udp", strconv.Itoa(heartbeatPort))
	if err != nil {
		glog.Warning("Unable to resolve my UDP address")
	}
	heartbeatconn, err := net.ListenUDP("udp", udpAddress)
	if err != nil {
		glog.Error("Unable to listen on the heartbeat port %s", heartbeatPort)
	}

	for {
		var buf [4]byte
		_, addr, err := heartbeatconn.ReadFromUDP(buf[0:])
		if err != nil {
			glog.Warning("Could not read heartbeat message")
		}
		children[addr.String()].timestamp = time.Now().Unix()
	}
}

func checkChildren() {
	for {
		currTime := time.Now().Unix()
		for ip, node := range children {
			if currTime - node.timestamp > 2 * heartbeatPeriod {
				glog.Warning("Haven't received heartbeat from %s since two heartbeat periods", ip)
				suspects = append(suspects, node.vid)
				go checkSuspicion(node.vid)
			}
		}
	}
	return
}

func sendMessage(vid int, message string) {

	var addr net.UDPAddr
	addr.IP = net.ParseIP(memberMap[vid].ip)
	addr.Port = otherPort

	conn, err := net.DialUDP("udp", nil, &addr)
	if err != nil {
		glog.Warning("Unable to send message to vid=%d ip=%s", vid, memberMap[vid].ip)
	}
	defer conn.Close()
	conn.Write([]byte(message))
	return
}

func sendMessageAddr(ip string, message string) {
	var addr net.UDPAddr
	addr.IP = net.ParseIP(ip)
	addr.Port = otherPort

	conn, err := net.DialUDP("udp", nil, &addr)
	if err != nil {
		glog.Warning("Unable to send message to ip=%s", ip)
	}
	defer conn.Close()
	conn.Write([]byte(message))
	return

}

func checkSuspicion(vid int) {
	n := len(memberMap)
	pred := (vid - 1) % n
	succ1 := (vid + 1) % n
	succ2 := (vid + 2) % n

	for _, nvid := range([]int{pred, succ1, succ2}) {
		if nvid == myVid {
			continue
		}
		message := fmt.Sprintf("SUSPECT%s%d", delimiter, vid)
		sendMessage(nvid, message)
		
		// conn, err := net.DialUDP("udp", nil, memberMap[nvid].ip + otherPort)
		// if err != nil {
		// 	glog.Warning("[Suspect check] Cannot connect with vid=%d ip=%s", nvid, memberMap[nvid].ip)
		// }
		// defer conn.Close()
		// message := fmt.Sprintf("SUSPECT%s%d", delimiter, vid)
		// conn.Write(message)
	}
	time.Sleep(time.Duration(500) * time.Millisecond)
	for _, suspect := range(suspects) {
		if suspect == vid {
			// if it is still in the suspects list, mark as crashed and disseminate crash messages
		}
	}
	return
}

func massMail (message string) {
	for vid, node := range(memberMap) {
		if (vid == myVid || node.alive == false) {
			continue
		}
		sendMessage(vid, message)
	}
}

// func sendMemberMap(ip string) {
// 	// todo: send number of entries and try chunking - rn single entries

// 	var addr net.UDPAddr
// 	addr.IP = net.ParseIP(ip)
// 	addr.Port = otherPort

// 	conn, err := net.DialUDP("udp", nil, &addr)
// 	if err != nil {
// 		glog.Warning("Unable to send message to ip=%s", ip)
// 	}
// 	defer conn.Close()
// 	for vid, node := range(memberMap) {
// 		if node.alive == false {
// 			continue
// 		}
// 		message := fmt.Sprintf("MEMBER,%d,%s,%d", vid, node.ip, node.timestamp)
// 	}
// 	conn.Write([]byte(message))
// 	return

// }

func completeJoinRequests() (err error) {

	var addr net.UDPAddr
	addr.IP = net.ParseIP(introducer)
	addr.Port = introducerPort

	introducerConn, err := net.ListenUDP("udp", &addr)
	if err != nil {
		glog.Error("Unable to listen on the introducer port %s", introducerPort)
		return err
	}
	for {
		var buf [64]byte
		_, addr, err := introducerConn.ReadFromUDP(buf[0:])
		if err != nil {
			glog.Warning("Could not read  message on otherport %s", introducerPort)
		}

		fmt.Println("Received a join request from ip=%s", addr.IP.String())

		newVid := len(memberMap)

		var newnode MemberNode
		newnode.ip = addr.String()
		newnode.timestamp = time.Now().Unix()
		newnode.alive = true
		memberMap[newVid] = &newnode

		message := fmt.Sprintf("MEMBER,%d,%s,%d", 0, introducer, memberMap[0].timestamp)
		sendMessageAddr(newnode.ip, message)

		message = fmt.Sprintf("MEMBER,%d,%s,%d", newVid, newnode.ip, newnode.timestamp)
		sendMessageAddr(newnode.ip, message)

		fmt.Println("Sent member messages to the new node")

		// message = fmt.Sprintf("JOIN,%d,%s,%d", new_vid, newnode.ip, newnode.timestamp)
		// massMail(message) 

		// sendMemberMap(addr)


		// send entire membership list to the newly joined node





	}
	return nil
	
}

func listenOtherPort() (err error) {
	// setup other port
	var udpAddress net.UDPAddr
	udpAddress.IP = net.ParseIP(introducer)
	udpAddress.Port = introducerPort

	// udpAddress, err := net.ResolveUDPAddr("udp", myIP + ":" + strconv.Itoa(otherPort))
	// if err != nil { 
	// 	glog.Warning("Unable to resolve my UDP address")
	// 	return err
	// }
	otherportconn, err := net.ListenUDP("udp", &udpAddress)
	if err != nil {
		glog.Error("Unable to listen on the heartbeat port %s", otherPort)
		return err
	}

	fmt.Println("I am listening on otherport %d", otherPort)

	for {
		var buf [64]byte
		fmt.Println("Before Read from the otherport conn")
		n, addr, err := otherportconn.ReadFromUDP(buf[0:])
		fmt.Println("After Read from the otherport conn")

		if err != nil {
			glog.Warning("Could not read  message on otherport %s", otherPort)
		}

		message := string(buf[0:n])
		fmt.Println(message)
		split_message := strings.Split(message, delimiter)
		fmt.Println(message, split_message[0])
		message_type := split_message[0]
		subject, _ := strconv.Atoi(split_message[1])

		switch message_type {
		case "MEMBER":
			fmt.Println("Received a MEMBER message")
			var newnode MemberNode
			newnode.ip = split_message[2]
			newnode.timestamp, err = strconv.ParseInt(split_message[3], 10, 64)
			if err != nil {
				glog.Error("Cannot convert string timestamp to int64")
			}
			newnode.alive = true
			memberMap[subject] = &newnode 
			glog.Info("Added a new entry to the member map - vid=%d, ip=%s, timestamp=%d", subject, newnode.ip, newnode.timestamp)
			fmt.Printf("Added a new entry to the member map - vid=%d, ip=%s, timestamp=%d", subject, newnode.ip, newnode.timestamp)

		case "JOIN":
			var newnode MemberNode
			newnode.ip = split_message[2]
			newnode.timestamp, err = strconv.ParseInt(split_message[3], 10, 64)
			if err != nil {
				glog.Error("Cannot convert string timestamp to int64")
			}
			newnode.alive = true
			memberMap[subject] = &newnode

		case "LEAVE", "CRASH":
			memberMap[subject].alive = false

		case "SUSPECT":
			var found = false
			for _, vid := range(suspects) {
				if subject == vid {
					found = true
					break
				}
			}

			var message string

			if found {
				message = fmt.Sprintf("STATUS%s%d%s%d", delimiter, subject, delimiter, 1)
			} else {
				message = fmt.Sprintf("STATUS%s%d%s%d", delimiter, subject, delimiter, 0)
			}
			sendMessageAddr(addr.IP.String(), message)
		
		case "STATUS":
			status, _ := strconv.Atoi(split_message[2])
			if status == 1 {
				// declare node as crashed
				memberMap[subject].alive = false
				message = fmt.Sprintf("CRASH%s%d", delimiter, subject)
				massMail(message) 
			} else {
				// remove from suspects list
				for i, suspect := range(suspects) {
					if suspect == subject {
						suspects[i] = suspects[len(suspects)-1]
						suspects = suspects[:len(suspects)-1]
						break
					}
				}
			}
		}
	}
}

func sendJoinRequest() {
	var addr net.UDPAddr
	addr.IP = net.ParseIP(introducer)
	addr.Port = introducerPort

	conn, err := net.DialUDP("udp", nil, &addr)
	if err != nil {
		glog.Warning("Unable to send message to the introducer ip=%s", introducer)
	}
	message := "1"
	defer conn.Close()
	conn.Write([]byte(message))

	glog.Info("Sent a join request to the introducer %s", introducer)
	return
}

func getmyIP() (string) {
	var myip string
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		glog.Fatal("Cannot get my IP :(")
		os.Exit(1)
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


func main() {

	var wg sync.WaitGroup
	wg.Add(1)

	myIP = getmyIP()
	fmt.Println(myIP)
	if myIP == introducer {
		var node MemberNode
		node.ip = myIP
		node.timestamp = time.Now().Unix()
		node.alive = true
		memberMap[0] = &node
		
		go completeJoinRequests()

	} else{
		sendJoinRequest()
	}

	go listenOtherPort()

	wg.Wait()
	return
}





