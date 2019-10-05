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

// For storing the last message we get
// type TimerNode struct{
// 	timestamp int64
// }

// todo
// type Message struct {
// 	message_type 
// }


// var introducer = "172.22.152.106"
var introducer = "172.22.152.109"
var introducerPort = 8082

var myVid int
var otherPort = 8081
var myIP string

var memberMap = make(map[int]*MemberNode)

var monitors = make(map[string]*MonitorNode)
var children = make(map[int]*ChildNode)

var timerMap = make(map[int] int64) // Map to store the last time a meesage has been received

var fingerNode = make(map[int] int)

var initRun = true // To run updateFinger table


var heartbeatPort = 8080
var heartbeatPeriod int64 = 2
var fingerPeriod int64 = 10
var suspects []int 	// remove from suspects when leave or crash 
var garbage []int

var delimiter = ","

func sendHeartbeat() {
	for {
		for type_, node := range(monitors) {
			_, err := node.conn.Write([]byte(strconv.Itoa(myVid)))
			if err != nil {
				glog.Warning("[HEARTBEAT] Could not send heartbeat to ", type_, node.vid)
			}
			glog.Infof("[HEARTBEAT %d] Sent heartbeat to %s %d", myVid, type_, node.vid)
		}
		time.Sleep(time.Duration(heartbeatPeriod) * time.Second)
	}
}

func receiveHeartbeat() {
	var myaddr net.UDPAddr
	myaddr.IP = net.ParseIP(myIP)
	myaddr.Port = heartbeatPort

	heartbeatConn, err := net.ListenUDP("udp", &myaddr)
	if err != nil {
		glog.Errorf("[HEARTBEAT %d] Unable to setup Listen on heartbeat port %d\n", myVid, heartbeatPort)
	}

	for {
		var buf [512]byte
		n, addr, err := heartbeatConn.ReadFromUDP(buf[0:])
		if err != nil {
			glog.Warningf("[HEARTBEAT %d] Could not read message on heartbeat port\n", myVid, heartbeatPort)
		}

		child_vid, err := strconv.Atoi(string(buf[0:n]))
		if err != nil {
			glog.Errorf("[HEARTBEAT %d] Could not map a heartbeat message to a virtual ID\n", myVid)
		}

		// Check if the sender vid is in your children map
		_, ok := children[child_vid]
		if ok {
			children[child_vid].timestamp = time.Now().Unix()
			glog.Infof("[HEARTBEAT %d] Received heartbeat from vid=%d, ip=%s\n", myVid, child_vid, addr.IP.String())
		} else{
			glog.Infof("[HEARTBEAT %d] Received a non-child heartbeat from vid=%s, ip=%s", myVid, child_vid, addr.IP.String())

		}
	}
}

func checkChildren() {
	for {
		currTime := time.Now().Unix()
		for child_vid, cnode := range children {
			if currTime - cnode.timestamp > 2 * heartbeatPeriod {
				glog.Warningf("[**SUSPICION %d **] Haven't received heartbeat from %s since two heartbeat periods\n", myVid, child_vid)
				suspects = append(suspects, child_vid)
				go checkSuspicion(child_vid)
			}
		}
		time.Sleep(time.Duration(heartbeatPeriod/2) * time.Second)
	}
	return
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
			if myIP == introducer {
				go addToDead(vid)
			}

			_, ok := children[vid]
			if ok {
				delete(children, vid)
			}

			message := fmt.Sprintf("CRASH,%d,%d", vid,time.Now().Unix())
			disseminate(message)		
			// massMail(message)
			updateMonitors()
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

func sendMessage(vid int, message string) {
	var addr net.UDPAddr
	addr.IP = net.ParseIP(memberMap[vid].ip)
	addr.Port = otherPort

	conn, err := net.DialUDP("udp", nil, &addr)
	if err != nil {
		glog.Warningf("[SEND MESSAGE] Unable to send message to vid=%d ip=%s", vid, memberMap[vid].ip)
	}
	defer conn.Close()
	conn.Write([]byte(message))
	// glog.Info("Sent message %s to %s", message, vid)
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
	pred := mod(vid - 1, n)
	for {
		if memberMap[pred].alive == true {
			if pred != vid {
				break
			}
		}
		pred = mod(pred - 1, n)
	}

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


func updateFingerTable() {
	for{ // Infitnite Loop

		n := len(memberMap)
		glog.Infof("[FINGER %d] Updating the finger table, memberMap Len %d",myVid,n)
		mult := 1
		idx := 0

		for{

			newVid := getSuccessor(( (myVid -1)+mult)%n)
			fingerNode[idx] = newVid
			idx = idx+1
			mult = mult*2
			if (mult >= n){
				break
			}
		}

		eidx := len(fingerNode)
		for i:= idx ; i < eidx; i++{
			delete(fingerNode,idx)
		}

		time.Sleep(time.Duration(fingerPeriod) * time.Second)

	}

	return
}

func disseminate(message string) {

	// Send to the neighbourhood Nodes, code from heartbeat
	for _, node := range(monitors) {
		sendMessage(node.vid,message)
	}

	for _, node := range(fingerNode) {
		sendMessage(node,message)
	}// Not added yet
	
}

func massMail(message string) {
	for vid, node := range(memberMap) {
		if (vid == myVid || node.alive == false) {
			continue
		}
		sendMessage(vid, message)
	}
}

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

// func setNewMonitors() {
// 		newpred := getPredecessor(myVid)
// 		if newpred != monitors["pred"].vid {
// 			oldpred := monitors["pred"].vid
// 			monitor_node := createMonitor(newpred)
// 			monitors["pred"] = &monitor_node

// 			message := fmt.Sprintf("ADD,%d,%s,%d", myVid, memberMap[myVid].ip, memberMap[myVid].timestamp)
// 			sendMessage(newpred, message)
// 			message = fmt.Sprintf("REMOVE,%d", myVid)
// 			sendMessage(oldpred, message)
// 		}

// 		newsucc1 := getSuccessor(myVid)
// 		if newsucc1 != monitors["succ1"].vid {
// 			oldsucc1 := monitors["succ1"].vid
// 			monitor_node := createMonitor(newsucc1)
// 			monitors["succ1"] = &monitor_node

// 			message := fmt.Sprintf("ADD,%d,%s,%d", myVid, memberMap[myVid].ip, memberMap[myVid].timestamp)
// 			sendMessage(newsucc1, message)
// 			message = fmt.Sprintf("REMOVE,%d", myVid)
// 			sendMessage(oldsucc1, message)
// 		}

// 		newsucc2 := getSuccessor2(myVid)
// 		if newsucc2 != monitors["succ2"].vid {
// 			oldsucc2 := monitors["succ2"].vid
// 			monitor_node := createMonitor(newsucc2)
// 			monitors["succ2"] = &monitor_node

// 			message := fmt.Sprintf("ADD,%d,%s,%d", myVid, memberMap[myVid].ip, memberMap[myVid].timestamp)
// 			sendMessage(newsucc2, message)
// 			message = fmt.Sprintf("REMOVE,%d", myVid)
// 			sendMessage(oldsucc2, message)
// 		}

// 		return
// }

func completeJoinRequests() (err error) {
	var myaddr net.UDPAddr
	myaddr.IP = net.ParseIP(introducer)
	myaddr.Port = introducerPort

	introducerConn, err := net.ListenUDP("udp", &myaddr)
	if err != nil {
		glog.Errorf("[INTRODUCER] Unable to setup Listen on the introducer port %s", introducerPort)
		return err
	}

	for {
		var buf [512]byte
		_, addr, err := introducerConn.ReadFromUDP(buf[0:])
		if err != nil {
			glog.Warningf("[INTRODUCER] Could not read message on the otherport %s", introducerPort)
		}

		glog.Infof("Received a join request from ip=%s", addr.IP.String())

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

		message := fmt.Sprintf("YOU,%d,%s,%d", newVid, newnode.ip, newnode.timestamp)
		sendMessage(newVid, message)
		// Message about me should clome first

		message = fmt.Sprintf("MEMBER,0,%s,%d", introducer, memberMap[0].timestamp)
		sendMessage(newVid, message)



		findAndSendMonitors(newVid)

		if initRun{ // global variable
			go updateFingerTable()
			initRun = false
		}
		

		time.Sleep(100 * time.Millisecond)

		message = fmt.Sprintf("JOIN,%d,%s,%d,%d", newVid, newnode.ip, newnode.timestamp,time.Now().Unix())
		disseminate(message)
		// massMail(message)

		// newpred := getPredecessor(myVid)
		// _, ok := monitors["pred"]
		// if ok {
		// 	if newpred != monitors["pred"].vid {
		// 		oldpred := monitors["pred"].vid
		// 		monitor_node := createMonitor(newpred)
		// 		monitors["pred"] = &monitor_node

		// 		message := fmt.Sprintf("ADD,%d,%s,%d", myVid, memberMap[myVid].ip, memberMap[myVid].timestamp)
		// 		sendMessage(newpred, message)
		// 		message = fmt.Sprintf("REMOVE,%d", myVid)
		// 		sendMessage(oldpred, message)
		// 	}
		// } else {
		// 	monitor_node := createMonitor(newpred)
		// 	monitors["pred"] = &monitor_node

		// 	message := fmt.Sprintf("ADD,%d,%s,%d", myVid, memberMap[myVid].ip, memberMap[myVid].timestamp)
		// 	sendMessage(newpred, message)
		// } 

		// newsucc1 := getSuccessor(myVid)
		// _, ok = monitors["succ1"]
		// if ok {
		// 	if newsucc1 != monitors["succ1"].vid {
		// 		oldsucc1 := monitors["succ1"].vid
		// 		monitor_node := createMonitor(newsucc1)
		// 		monitors["succ1"] = &monitor_node

		// 		message := fmt.Sprintf("ADD,%d,%s,%d", myVid, memberMap[myVid].ip, memberMap[myVid].timestamp)
		// 		sendMessage(newsucc1, message)
		// 		message = fmt.Sprintf("REMOVE,%d", myVid)
		// 		sendMessage(oldsucc1, message)
		// 	}
		// } else{
		// 	monitor_node := createMonitor(newsucc1)
		// 	monitors["succ1"] = &monitor_node

		// 	message := fmt.Sprintf("ADD,%d,%s,%d", myVid, memberMap[myVid].ip, memberMap[myVid].timestamp)
		// 	sendMessage(newsucc1, message)
		// }

		// newsucc2 := getSuccessor2(myVid)
		// _, ok = monitors["succ2"]
		// if ok {
		// 	if newsucc2 != monitors["succ2"].vid {
		// 		oldsucc2 := monitors["succ2"].vid
		// 		monitor_node := createMonitor(newsucc2)
		// 		monitors["succ2"] = &monitor_node

		// 		message := fmt.Sprintf("ADD,%d,%s,%d", myVid, memberMap[myVid].ip, memberMap[myVid].timestamp)
		// 		sendMessage(newsucc2, message)
		// 		message = fmt.Sprintf("REMOVE,%d", myVid)
		// 		sendMessage(oldsucc2, message)
		// 	}
		// } else{
		// 	monitor_node := createMonitor(newsucc2)
		// 	monitors["succ2"] = &monitor_node

		// 	message := fmt.Sprintf("ADD,%d,%s,%d", myVid, memberMap[myVid].ip, memberMap[myVid].timestamp)
		// 	sendMessage(newsucc2, message)
		// }

		updateMonitors()
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

func Difference(a, b []int) (diff []int) {
      m := make(map[int]bool)

      for _, item := range b {
              m[item] = true
      }

      for _, item := range a {
              if _, ok := m[item]; !ok {
                      diff = append(diff, item)
              }
      }
      return
}

func updateMonitors() {

	var old_monitors []int
	var new_monitors []int

	newpred := getPredecessor(myVid)
	new_monitors = append(new_monitors, newpred)

	_, ok := monitors["pred"]

	if !ok {
		monitor_node := createMonitor(newpred)
		monitors["pred"] = &monitor_node

		// message := fmt.Sprintf("ADD,%d,%s,%d", myVid, memberMap[myVid].ip, memberMap[myVid].timestamp)
		// sendMessage(newpred, message)
	} else {
		oldpred := monitors["pred"].vid
		old_monitors = append(old_monitors, oldpred)

		if oldpred != newpred {
			monitor_node := createMonitor(newpred)
			monitors["pred"] = &monitor_node

			// message := fmt.Sprintf("ADD,%d,%s,%d", myVid, memberMap[myVid].ip, memberMap[myVid].timestamp)
			// sendMessage(newpred, message)

			// message = fmt.Sprintf("REMOVE,%d", myVid)
			// sendMessage(oldpred, message)
		}
	}

	newsucc1 := getSuccessor(myVid)
	new_monitors = append(new_monitors, newsucc1)

	_, ok = monitors["succ1"]
	if !ok {
		monitor_node := createMonitor(newsucc1)
		monitors["succ1"] = &monitor_node

		// message := fmt.Sprintf("ADD,%d,%s,%d", myVid, memberMap[myVid].ip, memberMap[myVid].timestamp)
		// sendMessage(newsucc1, message)
	} else {
		oldsucc1 := monitors["succ1"].vid
		old_monitors = append(old_monitors, oldsucc1)

		if newsucc1 != oldsucc1 {
			monitor_node := createMonitor(newsucc1)
			monitors["succ1"] = &monitor_node

			// message := fmt.Sprintf("ADD,%d,%s,%d", myVid, memberMap[myVid].ip, memberMap[myVid].timestamp)
			// sendMessage(newsucc1, message)

			// message = fmt.Sprintf("REMOVE,%d", myVid)
			// sendMessage(oldsucc1, message)
		}
	}

	newsucc2 := getSuccessor2(myVid)
	new_monitors = append(new_monitors, newsucc2)

	_, ok = monitors["succ2"]
	if !ok {
		monitor_node := createMonitor(newsucc2)
		monitors["succ2"] = &monitor_node

		// message := fmt.Sprintf("ADD,%d,%s,%d", myVid, memberMap[myVid].ip, memberMap[myVid].timestamp)
		// sendMessage(newsucc2, message)
	} else {
		oldsucc2 := monitors["succ2"].vid
		old_monitors = append(old_monitors, oldsucc2)

		if newsucc2 != oldsucc2 {
			monitor_node := createMonitor(newsucc2)
			monitors["succ2"] = &monitor_node

			// message := fmt.Sprintf("ADD,%d,%s,%d", myVid, memberMap[myVid].ip, memberMap[myVid].timestamp)
			// sendMessage(newsucc2, message)

			// message = fmt.Sprintf("REMOVE,%d", myVid)
			// sendMessage(oldsucc2, message)
		}
	}

	to_add := Difference(new_monitors, old_monitors)
	for _, vid := range(to_add) {
		message := fmt.Sprintf("ADD,%d,%s,%d", myVid, memberMap[myVid].ip, memberMap[myVid].timestamp)
		sendMessage(vid, message)
	}

	to_remove := Difference(old_monitors, new_monitors)
	for _, vid := range(to_remove) {
		message := fmt.Sprintf("REMOVE,%d", myVid)
		sendMessage(vid, message)
	}
}





func listenOtherPort() (err error) {
	var myaddr net.UDPAddr
	myaddr.IP = net.ParseIP(myIP)
	myaddr.Port = otherPort

	otherportConn, err := net.ListenUDP("udp", &myaddr)
	glog.Info("Listening on port", otherPort)

	if err != nil {
		glog.Error("Unable to listen on the otherport port %s", otherPort)
		return err
	}

	for {
		var buf [512]byte
		n, addr, err := otherportConn.ReadFromUDP(buf[0:])
		if err != nil {
			glog.Warning("Could not read  message on otherport %s", otherPort)
		}

		message := string(buf[0:n])
		split_message := strings.Split(message, delimiter)
		message_type := split_message[0]
		subject, _ := strconv.Atoi(split_message[1])

		glog.Info(message)

		switch message_type {
		case "ADD":
			var cnode ChildNode
			cnode.timestamp = time.Now().Unix()
			children[subject] = &cnode
			
			var newnode MemberNode
			newnode = createMember(split_message[2], split_message[3])
			memberMap[subject] = &newnode


		case "REMOVE":
			_, ok := children[subject]
			if ok {
				delete(children, subject)
			}

		case "PRED", "SUCC1", "SUCC2":
			var newnode MemberNode
			newnode = createMember(split_message[2], split_message[3])
			memberMap[subject] = &newnode

			var node MonitorNode
			node = createMonitor(subject)
			monitors[strings.ToLower(message_type)] = &node

			// Send an ADD message to monitor
			message := fmt.Sprintf("ADD,%d,%s,%d", myVid, memberMap[myVid].ip, memberMap[myVid].timestamp)
			sendMessageAddr(newnode.ip, message)

		case "YOU":
			myVid = subject
			var newnode MemberNode
			newnode = createMember(split_message[2], split_message[3])
			memberMap[subject] = &newnode

			if initRun {
				go updateFingerTable()
				initRun = false
			}

		case "MEMBER":
			if subject == myVid {
				break
			}
			newnode := createMember(split_message[2], split_message[3])
			memberMap[subject] = &newnode



		case "JOIN":
			if subject == myVid {
				break
			}

			newnode := createMember(split_message[2], split_message[3])
			memberMap[subject] = &newnode

			message := fmt.Sprintf("MEMBER,%d,%s,%d", myVid, myIP, memberMap[myVid].timestamp)
			sendMessage(subject, message)
			updateMonitors()

			// Read the sender of the message
			len_msg := len(split_message)
			origin_time,_ := strconv.ParseInt(string(split_message[len_msg-1]), 10, 64)
			

			// Check the timerMap
			_,ok := timerMap[subject]
			if (ok || timerMap[subject] < origin_time){
				timerMap[subject] = origin_time
				disseminate(message)
			} 


		case "LEAVE", "CRASH":
			// mark crashed

			// check if it is currently marked alive or not
			glog.Infof("Received CRASH for %d",subject)
			_,ok := memberMap[subject]
			if ok {

				if memberMap[subject].alive == false{
					break;
				}
				memberMap[subject].alive = false
				if myIP == introducer {
					go addToDead(subject)
				}
				
				
				_, ok = children[subject]
				if ok {
					delete(children, subject)
				}
				updateMonitors()
			}

			// Read the sender of the message
			len_msg := len(split_message)
			origin_time,_ := strconv.ParseInt(string(split_message[len_msg-1]), 10, 64)

			// Check the timerMap
			_,ok = timerMap[subject]
			if (ok || timerMap[subject] < origin_time){
				timerMap[subject] = origin_time
				disseminate(message)
			} 

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
			if status == 1 {
				suspect_idx := -1
				for i, suspect := range(suspects) {
					if suspect == subject {
						suspect_idx = i
						break
					}
				}
				if suspect_idx != -1 {
					suspects[suspect_idx] = suspects[len(suspects)-1]
					suspects = suspects[:len(suspects)-1]
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
		glog.Warningf("Unable to send message to the introducer ip=%s", introducer)
	}
	message := "1"
	defer conn.Close()
	conn.Write([]byte(message))

	glog.Infof("Sent a join request to the introducer %s", introducer)
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





