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
}

// todo
// type Message struct {
// 	message_type 
// }

var monitors = make(map[string]*MonitorNode)
var memberMap = make(map[int]*MemberNode)
var heartbeatPort = 8080
var myVid int	// this will be populated after join by introducer
var heartbeatPeriod int64 = 2	// in seconds
var children = make(map[string]*ChildNode)
var suspects []int 	// remove from suspects when leave or crash 
var otherPort = 8081
var delimiter = ","
var myIP string
var introducer = "172.22.152.106"
var introducerPort = 8082
var garbage []int

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
			_, err := node.conn.Write([]byte(myVid))
			if err != nil {
				glog.Warning("Could not send heartbeat myip=%s myvid=%d", myIP, myVid)
			}
		}
		time.Sleep(time.Duration(heartbeatPeriod) * time.Second)
	}
}

func receiveHeartbeat() {

	var myaddr net.UDPAddr
	myaddr.IP = net.ParseIP(myIP)
	myaddr.Port = heartbeatPort

	heartbeatconn, err := net.ListenUDP("udp", &myaddr)
	if err != nil {
		glog.Error("Unable to listen on the heartbeat port %s", heartbeatPort)
	}

	for {
		var buf [4]byte
		n, addr, err := heartbeatconn.ReadFromUDP(buf[0:])
		if err != nil {
			glog.Warning("Could not read heartbeat message")
		}
		child_vid, err := strconv.Atoi(buf[0:n])
		if err != nil {
			glog.Error("Could not convert heartbeart message to a virtual ID\n")
		}
		_, ok = children[child_vid]
		if ok {
			children[child_vid].timestamp = time.Now().Unix()
			glog.Info("Received heartbeat from vid=%d, ip=%s\n", child_vid, addr.IP.String())
		} else{
			glog.Info("Received a RANDOM heartbeat from vid=%s, ip=%s", child_vid, addr.IP.String())

		}
	}
}

func checkChildren() {
	for {
		currTime := time.Now().Unix()
		for child_vid, cnode := range children {
			if currTime - cnode.timestamp > 2 * heartbeatPeriod {
				glog.Warning("Haven't received heartbeat from %s since two heartbeat periods", child_vid)
				suspects = append(suspects, cnode.vid)
				go checkSuspicion(cnode.vid)
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
	_, err = conn.Write([]byte(message))
	if err != nil {
		fmt.Println(err, "Unable to write member messages")
	}
	fmt.Println("Successfully wrote member message")
	return

}

func getPredecessor(vid int) {
	n := len(memberMap)
	pred := (vid - 1) % n
	for {
		if memberMap[pred].alive == true {
			break
		}
		pred = (pred - 1) % n
	}
	return pred
}

func getSuccessor(vid int) {
	n := len(memberMap)

	succ1 := (vid + 1) % n
	for {
		if memberMap[succ1].alive == true {
			break
		}
		succ1 = (succ1 + 1) % n
	}
	return succ1
}

func checkSuspicion(vid int) {
	pred := getPredecessor(vid)
	succ1 := getSuccessor(vid)
	succ2 := getSuccessor(succ1)

	for _, nvid := range([]int{pred, succ1, succ2}) {
		if nvid == myVid {
			continue
		}
		message := fmt.Sprintf("SUSPECT%s%d", delimiter, vid)
		sendMessage(nvid, message)
	}
	time.Sleep(time.Duration(500) * time.Millisecond)
	for _, suspect := range(suspects) {
		if suspect == vid {
			message = fmt.Sprintf("CRASH,%d", vid)
			massMail(message) 
			break
		}
	}
	return
}

func massMail(message string) {
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

func findAndSendMonitors(vid int) {
	var pred, succ1, succ2, n int
	// n = len(memberMap)
	pred = getPredecessor(vid)
	message = fmt.Sprintf("PRED,%d,%s,%s", pred, memberMap[pred].ip, memberMap[pred].timestamp)
	sendMessage(vid, message)

	succ1 = getSuccessor(vid)
	message = fmt.Sprintf("SUCC1,%d,%s,%s", succ1, memberMap[succ1].ip, memberMap[succ1].timestamp)
	sendMessage(vid, message)

	succ2 = getSuccessor(succ1)
	message = fmt.Sprintf("SUCC2,%d,%s,%s", succ2, memberMap[succ2].ip, memberMap[succ2].timestamp)
	sendMessage(vid, message)
}


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

		var newVid int

		if len(garbage) == 0 {
			newVid = len(memberMap)
		} else {
			newVid = garbage[0]
			garbage = garbage[1:]
		}

		var newnode MemberNode
		newnode.ip = addr.IP.String()
		newnode.timestamp = time.Now().Unix()
		newnode.alive = true
		memberMap[newVid] = &newnode

		message := fmt.Sprintf("MEMBER,0,%s,%d", introducer, memberMap[0].timestamp)
		sendMessageAddr(newnode.ip, message)

		message = fmt.Sprintf("MEMBER,%d,%s,%d", newVid, newnode.ip, newnode.timestamp)
		sendMessageAddr(newnode.ip, message)

		// send pred, succ
		findAndSendMonitors(newVid)

		message = fmt.Sprintf("JOIN,%d,%s,%d", newVid, newnode.ip, newnode.timestamp)
		massMail(message)
	}
	return nil
	
}

func addToDead(vid int) {
	time.Sleep(6 * time.Second)
	garbage = append(garbage, vid)
}

func listenOtherPort() (err error) {
	var myaddr net.UDPAddr
	myaddr.IP = net.ParseIP(myIP)
	myaddr.Port = otherPort

	otherportconn, err := net.ListenUDP("udp", &myaddr)

	if err != nil {
		glog.Error("Unable to listen on the otherport port %s", otherPort)
		return err
	}

	for {
		var buf [64]byte
		n, addr, err := otherportconn.ReadFromUDP(buf[0:])

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
		case "ADD":
			var cnode ChildNode
			cnode.timestamp = time.Now().Unix()
			children[subject] = &cnode 

			var newnode MemberNode
			newnode.ip = split_message[2]
			newnode.timestamp, err = strconv.ParseInt(string(split_message[3]), 10, 64)
			if err != nil {
				glog.Error("Cannot convert string timestamp to int64")
			}
			newnode.alive = true
			memberMap[subject] = &newnode

		case "REMOVE":
			_, ok := children[subject]
			if ok {
				delete(children, subject)
			}

		//todo condense handling of PRED, SUCC1, SUCC2
		case "PRED":
			// add to monitors and modify membersMap
			var node monitorNode
			node.vid = subject
			node.ip = split_message[2]

			var pred_addr net.UDPAddr
			pred_addr.IP = net.ParseIP(node.ip)
			pred_addr.Port = heartbeatPort

			node.conn, err = net.DialUDP("udp", nil, &pred_addr)

			monitorMap["pred"] = &node

			var newnode MemberNode
			newnode.ip = split_message[2]
			newnode.timestamp, err = strconv.ParseInt(string(split_message[3]), 10, 64)
			if err != nil {
				glog.Error("Cannot convert string timestamp to int64")
			}
			newnode.alive = true
			memberMap[subject] = &newnode

			message := fmt.Sprintf("ADD,%d,%s,%d", myVid, memberMap[myVid].ip, memberMap[myVid].timestamp)
			sendMessageAddr(pred_addr.IP, message)

		case "SUCC1":
			var node monitorNode
			node.vid = subject
			node.ip = split_message[2]

			var succ1_addr net.UDPAddr
			succ1_addr.IP = net.ParseIP(node.ip)
			succ1_addr.Port = heartbeatPort

			node.conn, err = net.DialUDP("udp", nil, &succ1_addr)

			monitorMap["succ1"] = &node

			var newnode MemberNode
			newnode.ip = split_message[2]
			newnode.timestamp, err = strconv.ParseInt(string(split_message[3]), 10, 64)
			if err != nil {
				glog.Error("Cannot convert string timestamp to int64")
			}
			newnode.alive = true
			memberMap[subject] = &newnode

			message := fmt.Sprintf("ADD,%d,%s,%d", myVid, memberMap[myVid].ip, memberMap[myVid].timestamp)
			sendMessageAddr(succ1_addr.IP, message)


		case "SUCC2":
			var node monitorNode
			node.vid = subject
			node.ip = split_message[2]

			var succ2_addr net.UDPAddr
			succ2_addr.IP = net.ParseIP(node.ip)
			succ2_addr.Port = heartbeatPort

			node.conn, err = net.DialUDP("udp", nil, &succ2_addr)

			monitorMap["succ2"] = &node

			var newnode MemberNode
			newnode.ip = split_message[2]
			newnode.timestamp, err = strconv.ParseInt(string(split_message[3]), 10, 64)
			if err != nil {
				glog.Error("Cannot convert string timestamp to int64")
			}
			newnode.alive = true
			memberMap[subject] = &newnode

			message := fmt.Sprintf("ADD,%d,%s,%d", myVid, memberMap[myVid].ip, memberMap[myVid].timestamp)
			sendMessageAddr(succ2_addr.IP, message)


		case "MEMBER":
			fmt.Println("[MEMBER] %d %s", subject, split_message[2])
			var newnode MemberNode
			newnode.ip = split_message[2]
			newnode.timestamp, err = strconv.ParseInt(string(split_message[3]), 10, 64)
			if err != nil {
				glog.Error("Cannot convert string timestamp to int64")
			}
			newnode.alive = true
			memberMap[subject] = &newnode 
			glog.Info("Added a new entry to the member map - vid=%d, ip=%s, timestamp=%d", subject, newnode.ip, newnode.timestamp)


		case "JOIN":
			glog.Info("[JOIN] %d %s", subject, split_message[2])

			var newnode MemberNode
			newnode.ip = split_message[2]
			newnode.timestamp, err = strconv.ParseInt(split_message[3], 10, 64)
			if err != nil {
				glog.Error("Cannot convert string timestamp to int64")
			}
			newnode.alive = true
			memberMap[subject] = &newnode

			message = fmt.Sprintf("MEMBER,%d,%s,%d", 0, myVid, myIP, memberMap[myVid].timestamp)
			sendMessage(subject, message)

		case "LEAVE", "CRASH":
			memberMap[subject].alive = false
			if myIP == introducer {
				go addToDead(subject)
			}

			for type_, node := range(monitors) {
				if subject == node.vid {
					// Baby you're in trouble
					switch type_ {
					case "pred":
						new_pred = getPredecessor(myVid)
						monitors["pred"] = 
					case "succ1"
					}
				}
			}



		case "SUSPECT":
			var alive = false

			var currTime = time.Now().Unix()
			for child_vid, cnode := range(children) {
				if subject == child_vid {
					if currTime - cnode.timestamp < heartbeatPeriod {
						alive = true
					}
					break
				}
			}

			var message string
			if alive {
				message = fmt.Sprintf("STATUS,%d,1", subject, 1)
			} else {
				message = fmt.Sprintf("STATUS,%d,0", subject, 0)
			}
			sendMessageAddr(addr.IP.String(), message)
		
		case "STATUS":
			status, _ := strconv.Atoi(split_message[2])
			if status == 0 {
				// declare node as crashed
				memberMap[subject].alive = false
				if myIP == introducer {
					go addToDead(subject)
				}
				message = fmt.Sprintf("CRASH,%d", subject)
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





