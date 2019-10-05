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
	"flag"
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
var children = make(map[int]*ChildNode)
var suspects []int 	// remove from suspects when leave or crash 
var otherPort = 8081
var delimiter = ","
var myIP string
// var introducer = "172.22.152.106"
var introducer = "172.22.152.109"
var introducerPort = 8082
var garbage []int

// func getMonitor(vid int) (MonitorNode, err error) {
// 	var node MonitorNode
// 	node.vid = vid
// 	node.ip = memberMap[vid].ip

// 	var addr net.UDPAddr
// 	addr.IP = net.ParseIP(node.ip)
// 	addr.Port = heartbeatPort

// 	node.conn, err = net.DialUDP("udp", nil, &addr)
// 	if err != nil {
// 		glog.Warning("Cannot setup a UDP connection with monitor vid=%d ip=%s", node.vid, node.ip)
// 		return node, err
// 	}
// 	return node, nil
// }

// func setupMonitors() {
// 	n := len(memberMap)
// 	pred := (myVid - 1) % n
// 	succ1 := (myVid + 1) % n
// 	succ2 := (myVid + 2) % n

// 	for i, vid := range([]int{pred, succ1, succ2}) {
// 		node, err := getMonitor(vid)
// 		if err != nil {
// 			glog.Warning("Cannot setup a UDP connection with monitor vid=%d", vid)
// 		} else {
// 			monitors[i] = node
// 		}
// 	}
// 	return
// }

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
	glog.Info("Started sending heartbeats", )
	for {
		for _, node := range(monitors) {
			// dummy = strconv.FormatInt(time.Now().Unix(), 10)
			_, err := node.conn.Write([]byte(strconv.Itoa(myVid)))
			if err != nil {
				glog.Warning("Could not send heartbeat myip=%s myvid=%d", myIP, myVid)
			}
			glog.Info("Sent heartbeat to", node.vid)
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

	glog.Info("Started listening on the heartbeat port", heartbeatPort)

	for {
		var buf [512]byte
		n, addr, err := heartbeatconn.ReadFromUDP(buf[0:])
		if err != nil {
			glog.Warning("Could not read heartbeat message")
		}
		child_vid, err := strconv.Atoi(string(buf[0:n]))
		if err != nil {
			glog.Error("Could not convert heartbeart message to a virtual ID\n")
		}
		_, ok := children[child_vid]
		if ok {
			children[child_vid].timestamp = time.Now().Unix()
			glog.Info("Received heartbeat from vid=%d, ip=%s\n", child_vid, addr.IP.String())
		} else{
			glog.Info("Received a RANDOM heartbeat from vid=%s, ip=%s", child_vid, addr.IP.String())

		}
	}
}

func checkChildren() {
	glog.Info("Started tracker to detect failures\n")

	for {
		currTime := time.Now().Unix()
		for child_vid, cnode := range children {
			if currTime - cnode.timestamp > 2 * heartbeatPeriod {
				glog.Warning("Haven't received heartbeat from %s since two heartbeat periods", child_vid)
				suspects = append(suspects, child_vid)
				go checkSuspicion(child_vid)
			}
		}
		time.Sleep(time.Duration(heartbeatPeriod/2) * time.Second)
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
	glog.Info("Sent message %s to %s", message, vid)
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
		fmt.Printf("Unable to write message %s", message)
	}	
	glog.Info("Sent message %s to %s", message, ip)

	return

}

func mod(a int, b int) int {
    m := a % b
    if a < 0 && b > 0 {
        m += b
    }
    return m
}

func getPredecessor(vid int) (int) {
	n := len(memberMap)
	glog.Info("n = ", n)
	pred := mod(vid - 1, n)
	glog.Info("pred = ", pred)
	for {
		if memberMap[pred].alive == true {
			if pred != vid {
				break
			}
		}
		pred = mod(pred - 1, n)
	}
	glog.Info("just beofe returning pred = ", pred)

	return pred
}

func getSuccessor(vid int) (int) {
	n := len(memberMap)

	succ := (vid + 1) % n
	for {
		if memberMap[succ].alive == true {
			if succ != vid {
				break
			}
		}
		succ = (succ + 1) % n
	}
	return succ
}

func getSuccessor2(vid int) (int) {
	succ1 := getSuccessor(vid)
	n := len(memberMap)

	succ2 := (succ1 + 1) % n
	for {
		if memberMap[succ2].alive == true {
			if succ2 != vid {
				break
			}
		}
		succ2 = (succ2 + 1) % n
	}
	return succ2
}

func checkSuspicion(vid int) {
	pred := getPredecessor(vid)
	succ1 := getSuccessor(vid)
	succ2 := getSuccessor(succ1)

	for _, nvid := range([]int{pred, succ1, succ2}) {
		if nvid == myVid {
			continue
		}
		glog.Info("Suspecting the Node : %d",vid)
		message := fmt.Sprintf("SUSPECT%s%d", delimiter, vid)
		sendMessage(nvid, message)
	}
	time.Sleep(time.Duration(500) * time.Millisecond) // time duration after which we said that this node is crashed

	suspect_idx := -1
	for i, suspect := range(suspects) {
		if suspect == vid {

			suspect_idx = i
			memberMap[suspect].alive = false
			message := fmt.Sprintf("CRASH,%d", vid)		
			massMail(message)
			setNewMonitors()
			break
		}
	}
	// remove dead node from suspects
	if suspect_idx != -1 {
		suspects[suspect_idx] = suspects[len(suspects)-1]
		suspects = suspects[:len(suspects)-1]
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
	var pred, succ1, succ2 int
	// n = len(memberMap)
	pred = getPredecessor(vid)
	message := fmt.Sprintf("PRED,%d,%s,%d", pred, memberMap[pred].ip, memberMap[pred].timestamp)
	sendMessage(vid, message)

	succ1 = getSuccessor(vid)
	message = fmt.Sprintf("SUCC1,%d,%s,%d", succ1, memberMap[succ1].ip, memberMap[succ1].timestamp)
	sendMessage(vid, message)

	succ2 = getSuccessor2(vid)
	message = fmt.Sprintf("SUCC2,%d,%s,%d", succ2, memberMap[succ2].ip, memberMap[succ2].timestamp)
	sendMessage(vid, message)
}


func setNewMonitors() {
		newpred := getPredecessor(myVid)
		if newpred != monitors["pred"].vid {
			oldpred := monitors["pred"].vid
			monitor_node := createMonitor(newpred)
			monitors["pred"] = &monitor_node

			message := fmt.Sprintf("ADD,%d,%s,%d", myVid, memberMap[myVid].ip, memberMap[myVid].timestamp)
			sendMessage(newpred, message)
			message = fmt.Sprintf("REMOVE,%d", myVid)
			sendMessage(oldpred, message)
		}

		newsucc1 := getSuccessor(myVid)
		if newsucc1 != monitors["succ1"].vid {
			oldsucc1 := monitors["succ1"].vid
			monitor_node := createMonitor(newsucc1)
			monitors["succ1"] = &monitor_node

			message := fmt.Sprintf("ADD,%d,%s,%d", myVid, memberMap[myVid].ip, memberMap[myVid].timestamp)
			sendMessage(newsucc1, message)
			message = fmt.Sprintf("REMOVE,%d", myVid)
			sendMessage(oldsucc1, message)
		}

		newsucc2 := getSuccessor2(myVid)
		if newsucc2 != monitors["succ2"].vid {
			oldsucc2 := monitors["succ2"].vid
			monitor_node := createMonitor(newsucc2)
			monitors["succ2"] = &monitor_node

			message := fmt.Sprintf("ADD,%d,%s,%d", myVid, memberMap[myVid].ip, memberMap[myVid].timestamp)
			sendMessage(newsucc2, message)
			message = fmt.Sprintf("REMOVE,%d", myVid)
			sendMessage(oldsucc2, message)
		}

		return
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
		var buf [512]byte
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

		// time.Sleep(5 * time.Second)

		message := fmt.Sprintf("MEMBER,0,%s,%d", introducer, memberMap[0].timestamp)
		sendMessage(newVid, message)

		message = fmt.Sprintf("YOU,%d,%s,%d", newVid, newnode.ip, newnode.timestamp)
		sendMessage(newVid, message)

		findAndSendMonitors(newVid)

		// time.Sleep(5 * time.Second)

		message = fmt.Sprintf("JOIN,%d,%s,%d", newVid, newnode.ip, newnode.timestamp)
		massMail(message)

		newpred := getPredecessor(myVid)
		_, ok := monitors["pred"]
		if ok {
			if newpred != monitors["pred"].vid {
				oldpred := monitors["pred"].vid
				monitor_node := createMonitor(newpred)
				monitors["pred"] = &monitor_node

				message := fmt.Sprintf("ADD,%d,%s,%d", myVid, memberMap[myVid].ip, memberMap[myVid].timestamp)
				sendMessage(newpred, message)
				message = fmt.Sprintf("REMOVE,%d", myVid)
				sendMessage(oldpred, message)
			}
		} else {
			monitor_node := createMonitor(newpred)
			monitors["pred"] = &monitor_node

			message := fmt.Sprintf("ADD,%d,%s,%d", myVid, memberMap[myVid].ip, memberMap[myVid].timestamp)
			sendMessage(newpred, message)
		} 

		newsucc1 := getSuccessor(myVid)
		_, ok = monitors["succ1"]
		if ok {
			if newsucc1 != monitors["succ1"].vid {
				oldsucc1 := monitors["succ1"].vid
				monitor_node := createMonitor(newsucc1)
				monitors["succ1"] = &monitor_node

				message := fmt.Sprintf("ADD,%d,%s,%d", myVid, memberMap[myVid].ip, memberMap[myVid].timestamp)
				sendMessage(newsucc1, message)
				message = fmt.Sprintf("REMOVE,%d", myVid)
				sendMessage(oldsucc1, message)
			}
		} else{
			monitor_node := createMonitor(newsucc1)
			monitors["succ1"] = &monitor_node

			message := fmt.Sprintf("ADD,%d,%s,%d", myVid, memberMap[myVid].ip, memberMap[myVid].timestamp)
			sendMessage(newsucc1, message)
		}

		newsucc2 := getSuccessor2(myVid)
		_, ok = monitors["succ2"]
		if ok {
			if newsucc2 != monitors["succ2"].vid {
				oldsucc2 := monitors["succ2"].vid
				monitor_node := createMonitor(newsucc2)
				monitors["succ2"] = &monitor_node

				message := fmt.Sprintf("ADD,%d,%s,%d", myVid, memberMap[myVid].ip, memberMap[myVid].timestamp)
				sendMessage(newsucc2, message)
				message = fmt.Sprintf("REMOVE,%d", myVid)
				sendMessage(oldsucc2, message)
			}
		} else{
			monitor_node := createMonitor(newsucc2)
			monitors["succ2"] = &monitor_node

			message := fmt.Sprintf("ADD,%d,%s,%d", myVid, memberMap[myVid].ip, memberMap[myVid].timestamp)
			sendMessage(newsucc2, message)
		}
	}
	return nil
	
}

func addToDead(vid int) {
	time.Sleep(6 * time.Second)
	garbage = append(garbage, vid)
}

func createMonitor(vid int) (MonitorNode) {
	var node MonitorNode
	node.vid = vid
	node.ip = memberMap[vid].ip

	var addr net.UDPAddr
	addr.IP = net.ParseIP(node.ip)
	addr.Port = heartbeatPort
	var err error
	node.conn, err = net.DialUDP("udp", nil, &addr)
	if err != nil {
		glog.Error("Unable to create a conn for monitor %d\n", vid)
	}
	return node
}

func createMember(ip string, str_timestamp string) (MemberNode){
	var node MemberNode
	node.ip = ip
	var err error
	node.timestamp, err = strconv.ParseInt(string(str_timestamp), 10, 64)
	if err != nil {
		glog.Error("Cannot convert string timestamp to int64")
	}
	node.alive = true
	return node
}


func listenOtherPort() (err error) {
	var myaddr net.UDPAddr
	myaddr.IP = net.ParseIP(myIP)
	myaddr.Port = otherPort

	otherportconn, err := net.ListenUDP("udp", &myaddr)
	glog.Info("Listening on port", otherPort)

	if err != nil {
		glog.Error("Unable to listen on the otherport port %s", otherPort)
		return err
	}

	for {
		var buf [512]byte
		// glog.Info("before")
		n, addr, err := otherportconn.ReadFromUDP(buf[0:])
		// glog.Info("after")

		if err != nil {
			glog.Warning("Could not read  message on otherport %s", otherPort)
		}

		message := string(buf[0:n])

		glog.Info(message)

		split_message := strings.Split(message, delimiter)
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
			newnode := createMember(split_message[2], split_message[3])
			memberMap[subject] = &newnode

			node := createMonitor(subject)			
			monitors["pred"] = &node

			message := fmt.Sprintf("ADD,%d,%s,%d", myVid, memberMap[myVid].ip, memberMap[myVid].timestamp)
			sendMessageAddr(newnode.ip, message)

		case "SUCC1":
			newnode := createMember(split_message[2], split_message[3])
			memberMap[subject] = &newnode

			node := createMonitor(subject)			
			monitors["succ1"] = &node

			message := fmt.Sprintf("ADD,%d,%s,%d", myVid, memberMap[myVid].ip, memberMap[myVid].timestamp)
			sendMessageAddr(newnode.ip, message)


		case "SUCC2":
			newnode := createMember(split_message[2], split_message[3])
			memberMap[subject] = &newnode

			node := createMonitor(subject)			
			monitors["succ2"] = &node

			message := fmt.Sprintf("ADD,%d,%s,%d", myVid, memberMap[myVid].ip, memberMap[myVid].timestamp)
			sendMessageAddr(newnode.ip, message)

		case "YOU":
			myVid = subject
			newnode := createMember(split_message[2], split_message[3])
			memberMap[subject] = &newnode
			glog.Info("Got my VID", myVid)

		case "MEMBER":
			// fmt.Println("[MEMBER] %d %s", subject, split_message[2])
			// var newnode MemberNode
			// newnode.ip = split_message[2]
			// newnode.timestamp, err = strconv.ParseInt(string(split_message[3]), 10, 64)
			// if err != nil {
			// 	glog.Error("Cannot convert string timestamp to int64")
			// }
			// newnode.alive = true
			// memberMap[subject] = &newnode
			if subject == myVid {
				break
			}
			newnode := createMember(split_message[2], split_message[3])
			memberMap[subject] = &newnode

			glog.Info("Added a new entry to the member map - vid=%d, ip=%s, timestamp=%d", subject, newnode.ip, newnode.timestamp)


		case "JOIN":
			if subject == myVid {
				break
			}
			glog.Info("[JOIN] %d %s", subject, split_message[2])

			newnode := createMember(split_message[2], split_message[3])
			memberMap[subject] = &newnode

			glog.Info(myVid, myIP)
			glog.Info(memberMap[myVid].timestamp)

			message := fmt.Sprintf("MEMBER,%d,%s,%d", myVid, myIP, memberMap[myVid].timestamp)
			sendMessage(subject, message)

			setNewMonitors()

			

		case "LEAVE", "CRASH":
			memberMap[subject].alive = false
			if myIP == introducer {
				go addToDead(subject)
			}
			glog.Info("Received CRASH for %d",subject)
			// Setting the node type to be dead
			// memberMap[subject].alive = false 

			setNewMonitors()



			// for type_, node := range(monitors) {
			// 	if subject == node.vid {
			// 		// Baby you're in trouble
			// 		var newMonitorID int
			// 		switch type_ {
			// 		case "pred":
			// 			newMonitorID = getPredecessor(myVid)
			// 		case "succ1":
			// 			newMonitorID = getSuccessor(myVid)
			// 		case "succ2":
			// 			newMonitorID = getSuccessor(monitors["succ2"].vid)
			// 		}
			// 		var node MonitorNode
			// 		node.vid = newMonitorID
			// 		node.ip = memberMap[newMonitorID].ip

			// 		var node_addr net.UDPAddr
			// 		node_addr.IP = net.ParseIP(node.ip)
			// 		node_addr.Port = heartbeatPort

			// 		node.conn, err = net.DialUDP("udp", nil, &node_addr)

			// 		monitors[type_] = &node

			// 		message := fmt.Sprintf("ADD,%d,%s,%d", myVid, memberMap[myVid].ip, memberMap[myVid].timestamp)
			// 		sendMessageAddr(node.ip, message)

			// 		break
			// 	}
			// }

			// Handle the crash part

		case "SUSPECT":
			var alive = false
			// Checked if it is set as dead in my list, if yes send dead message already
			if memberMap[subject].alive == false {
				alive = false
			} else{
				var currTime = time.Now().Unix()
				for child_vid, cnode := range(children) {
					if subject == child_vid {
						if currTime - cnode.timestamp < heartbeatPeriod {
							alive = true
						}
						break
					}
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
				// remove from suspect list and setup New monitors

				// remove from suspects list
				for i, suspect := range(suspects) {
					if suspect == subject {
						suspects[i] = suspects[len(suspects)-1]
						suspects = suspects[:len(suspects)-1]
						break
					}
				}
				message = fmt.Sprintf("CRASH,%d", subject)
				massMail(message) // massmail the nodes
				setNewMonitors()

	
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

func usage() {
	fmt.Fprintf(os.Stderr, "usage: example -stderrthreshold=[INFO|WARN|FATAL] -log_dir=[string]\n", )
	flag.PrintDefaults()
	os.Exit(2)
}

func init() {
	flag.Usage = usage
	// NOTE: This next line is key you have to call flag.Parse() for the command line 
	// options or "flags" that are defined in the glog module to be picked up.
	flag.Parse()
}


func main() {

	var wg sync.WaitGroup
	wg.Add(1)

	go listenOtherPort()

	time.Sleep(5 * time.Second)

	myIP = getmyIP()
	fmt.Println(myIP)
	if myIP == introducer {
		myVid = 0
		var node MemberNode
		node.ip = myIP
		node.timestamp = time.Now().Unix()
		node.alive = true
		memberMap[0] = &node
		go completeJoinRequests()
	} else{
		sendJoinRequest()
	}

	go sendHeartbeat()
	go receiveHeartbeat()
	go checkChildren()

	wg.Wait()
	return
}





