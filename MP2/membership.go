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
	"reflect"
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

var introducer = "172.22.152.106"
// var introducer = "172.22.152.109"
var introducerPort = 8082
var introPingPeriod = 5

var myVid int
var otherPort = 8081
var myIP string

var memberMap = make(map[int]*MemberNode)
var monitors = make(map[string]*MonitorNode)
var children = make(map[int]*ChildNode)

var eventTimeMap = make(map[int]int64)
var fingerTable = make(map[int]int)
var fingerTablePeriod int64 = 10

// [TODO] Not required
// var initRun = true // To run updateFinger table -- we can do away with this

var heartbeatPort = 8080
var heartbeatPeriod int64 = 2
var suspects []int 	// remove from suspects when leave or crash 

var garbage = make(map[int]bool)		// maintained by introducer to fill gaps

// [TODO] NOT required
var initMessageCount = 0

// [TODO] update whenever somebody with maxID leaves or crashes
var maxID = 0
var delimiter = ","

func sendHeartbeat() {
	for {
		for type_, node := range(monitors) {
			_, err := node.conn.Write([]byte(strconv.Itoa(myVid)))
			if err != nil {
				glog.Warningf("[HEARTBEAT %d] Could not send heartbeat to %s %d", myVid, type_, node.vid)
			}
			// glog.Infof("[HEARTBEAT %d] Sent heartbeat to %s %d", myVid, type_, node.vid)
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
		glog.Errorf("[HEARTBEAT %d] Unable to setup Listen on heartbeat port %d", myVid, heartbeatPort)
	}

	for {
		var buf [512]byte
		n, addr, err := heartbeatConn.ReadFromUDP(buf[0:])
		if err != nil {
			glog.Warningf("[HEARTBEAT %d] Could not read message on heartbeat port %d", myVid, heartbeatPort)
		}
		message := string(buf[0:n])
		child_vid, err := strconv.Atoi(message)
		if err != nil {
			glog.Errorf("[HEARTBEAT %d] Could not map a heartbeat message %s to a virtual ID\n", myVid, message)
		}

		// Check if the sender vid is in your children map
		_, ok := children[child_vid]
		if ok {
			children[child_vid].timestamp = time.Now().Unix()
			// glog.Infof("[HEARTBEAT %d] Received heartbeat from vid=%d, ip=%s\n", myVid, child_vid, addr.IP.String())
		} else{
			glog.Infof("[HEARTBEAT %d] Received a non-child heartbeat from vid=%s, ip=%s", myVid, child_vid, addr.IP.String())

		}
	}
}

func checkChildren() {
	for {
		currTime := time.Now().Unix()
		fmt.Printf("Children: ")
		for child_vid := range children {
			fmt.Printf("%d ", child_vid)
		}
		fmt.Printf("\n")

		for child_vid, cnode := range children {
			if currTime - cnode.timestamp > 2 * heartbeatPeriod {
				glog.Warningf("[HEARTBEAT %d] No heartbeat from %d since two heartbeat periods", myVid, child_vid)
				suspects = append(suspects, child_vid)
				go checkSuspicion(child_vid)
			}
		}
		time.Sleep(time.Duration(heartbeatPeriod/2) * time.Second)
	}
	return
}

func checkSuspicion(vid int) {
	// To check suspicion, query its neighbors
	pred := getPredecessor(vid)
	succ1 := getSuccessor(vid)
	succ2 := getSuccessor2(vid)

	for _, nvid := range([]int{pred, succ1, succ2}) {
		if nvid == myVid {
			continue
		}

		glog.Infof("[HEARTBEAT %d] Sending suspicion messages for %d", myVid, vid)

		message := fmt.Sprintf("SUSPECT,%d", vid)
		sendMessage(nvid, message)
	}

	// after 500 milliseconds, if the vid is still in suspects, declare it CRASHed
	time.Sleep(time.Duration(500) * time.Millisecond) 

	suspect_idx := -1
	for i, suspect := range(suspects) {
		if suspect == vid {
			suspect_idx = i
			memberMap[suspect].alive = false
			_, ok := children[vid]
			if ok {
				delete(children, vid)
			}
			crash_time := time.Now().Unix()
			message := fmt.Sprintf("CRASH,%d,%d", vid, crash_time)
			eventTimeMap[vid] = crash_time
			disseminate(message)
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
		glog.Warningf("Unable to send message to vid=%d ip=%s", vid, memberMap[vid].ip)
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
		glog.Warningf("[vid %d] Unable to write message to %s", myVid, ip)
	}

	return
}

func mod(a int, b int) int {
    m := a % b
    if a < 0 && b > 0 {
        m += b
    }
    return m
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
	
}

func getPredecessor(vid int) (int) {
	n := maxID + 1
	pred := mod(vid - 1, n)
	for {
		_, ok := memberMap[pred]
		if ok && memberMap[pred].alive == true {
			if pred != vid {
				break
			}
		}
		pred = mod(pred - 1, n)
	}
	return pred
}

func getSuccessor(vid int) (int) {
	n := maxID + 1
	succ := (vid + 1) % n
	for {
		_, ok := memberMap[succ] // checking if succ is in the memberMap
		if ok && memberMap[succ].alive == true {
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

	n := maxID + 1
	succ2 := (succ1 + 1) % n
	for {
		_, ok := memberMap[succ2]
		if ok && memberMap[succ2].alive == true {
			if succ2 != vid {
				break
			}
		}
		succ2 = (succ2 + 1) % n
	}
	return succ2
}

func updateFingerTable() {
	for {
		n := maxID + 1
		factor := 1
		idx := 0
		for {
			if (n < 2) {
				break
			}
			val := (myVid + factor) % n
			entry := getSuccessor(val)
			if (entry != myVid) {
				fingerTable[idx] = entry
				idx = idx + 1
			}
			factor = factor * 2
			if factor >= n {
				break
			}
		}

		stale_idx := len(fingerTable)
		for i:=idx; i<stale_idx; i++ {
			_, ok := fingerTable[i]
			if ok {
				delete(fingerTable, i)
			}
		}
		glog.Infof("[FINGER TABLE %d] Updated fingerTable to %v", myVid, fingerTable)
		time.Sleep(time.Duration(fingerTablePeriod) * time.Second)
	}
}

// func updateFingerTable() {
// 	for {
// 		n := maxID + 1
// 		glog.Infof("[FINGER %d] Updating the finger table, memberMap Len %d", myVid, n)
// 		mult := 1
// 		idx := 0

// 		for{
// 			if n < 2 {
// 				break
// 			}
// 			nval := (myVid+mult) % n
// 			newVid := getSuccessor(nval)
// 			fingerNode[idx] = newVid
// 			idx = idx+1
// 			mult = mult*2
// 			if (mult >= n){
// 				break
// 			}
// 		}

// 		eidx := len(fingerNode)
// 		if eidx > idx{
// 			for i:= idx ; i < eidx; i++{
// 				if n <2 {
// 					break
// 				}
// 				_,ok := fingerNode[i]
// 				if ok{
// 					delete(fingerNode,i)
// 				}
// 			}
// 		}

		
// 		glog.Infof("[FINGER %d] Updating the finger table, memberMap Len %d",myVid,len(fingerNode))

// 		time.Sleep(time.Duration(fingerPeriod) * time.Second)
// 	}
// 	return
// }

func disseminate(message string) {

	for _, node := range(monitors) {
		sendMessage(node.vid, message)
	}
	for _, finger := range(fingerTable) {
		if (finger == myVid || memberMap[finger].alive == false) {
			continue
		}
		sendMessage(finger,message)
	}
}


func checkIntroducer() {
	for {
		time.Sleep(time.Duration(introPingPeriod) * time.Second)
		if memberMap[0].alive == false {
			// If introducer is dead, periodically send your record to the introducer
			message:= fmt.Sprintf("INTRODUCER,%d,%s,%d,%d",myVid,memberMap[myVid].ip,memberMap[myVid].timestamp,maxID)
			sendMessage(0,message)
			// glog.Infof("[INTRODUCER %d] Sent message ",myVid)
		}
	
	}
}

// func massMail(message string) {
// 	for vid, node := range(memberMap) {
// 		if (vid == myVid || node.alive == false) {
// 			continue
// 		}
// 		sendMessage(vid, message)
// 	}
// }

func findAndSendMonitors(vid int) {
	var pred, succ1, succ2 int

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

func completeJoinRequests() (err error) {

	var myaddr net.UDPAddr
	myaddr.IP = net.ParseIP(introducer)
	myaddr.Port = introducerPort

	introducerConn, err := net.ListenUDP("udp", &myaddr)
	if err != nil {
		glog.Errorf("[INTRODUCER] Unable to setup Listen on the introducer port %d", introducerPort)
		return err
	}

	glog.Infof("[INTRODUCER] Started listening on the introducer port %d", introducerPort)

	for {
		var buf [512]byte
		_, addr, err := introducerConn.ReadFromUDP(buf[0:])
		if err != nil {
			glog.Warningf("[INTRODUCER] Could not read message on the introducer port %d", introducerPort)
		}

		glog.Infof("Received a join request from ip=%s", addr.IP.String())

		var newVid int

		if len(garbage) == 0 {
			maxID = maxID + 1
			newVid = maxID
		} else {
			for key := range(garbage) {
				newVid = key
				break
			}
			delete(garbage, newVid)
		}

		var newnode MemberNode
		newnode.ip = addr.IP.String()
		newnode.timestamp = time.Now().Unix()
		newnode.alive = true
		memberMap[newVid] = &newnode

		// Send the node's record
		message := fmt.Sprintf("YOU,%d,%s,%d", newVid, newnode.ip, newnode.timestamp)
		sendMessage(newVid, message)

		message = fmt.Sprintf("MEMBER,0,%s,%d", introducer, memberMap[0].timestamp)
		sendMessage(newVid, message)

		findAndSendMonitors(newVid)

		// if initRun { // global variable
		// 	go updateFingerTable()
		// 	initRun = false
		// }
		
		time.Sleep(100 * time.Millisecond)

		message = fmt.Sprintf("JOIN,%d,%s,%d", newVid, newnode.ip, newnode.timestamp)
		eventTimeMap[newVid] = newnode.timestamp
		disseminate(message)

		updateMonitors()
	}
	return nil
}

// func addToDead(vid int) {
// 	time.Sleep(6 * time.Second)
// 	garbage = append(garbage, vid)
// }

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

	old_monitors := []int{}
	new_monitors := []int{}

	newpred := getPredecessor(myVid)
	new_monitors = append(new_monitors, newpred)

	_, ok := monitors["pred"]

	if !ok {
		monitor_node := createMonitor(newpred)
		monitors["pred"] = &monitor_node
	} else {
		oldpred := monitors["pred"].vid
		old_monitors = append(old_monitors, oldpred)

		if oldpred != newpred {
			monitor_node := createMonitor(newpred)
			monitors["pred"] = &monitor_node
		}
	}

	newsucc1 := getSuccessor(myVid)
	new_monitors = append(new_monitors, newsucc1)

	_, ok = monitors["succ1"]
	if !ok {
		monitor_node := createMonitor(newsucc1)
		monitors["succ1"] = &monitor_node

	} else {
		oldsucc1 := monitors["succ1"].vid
		old_monitors = append(old_monitors, oldsucc1)

		if newsucc1 != oldsucc1 {
			monitor_node := createMonitor(newsucc1)
			monitors["succ1"] = &monitor_node
		}
	}

	newsucc2 := getSuccessor2(myVid)
	new_monitors = append(new_monitors, newsucc2)

	_, ok = monitors["succ2"]
	if !ok {
		monitor_node := createMonitor(newsucc2)
		monitors["succ2"] = &monitor_node
	} else {
		oldsucc2 := monitors["succ2"].vid
		old_monitors = append(old_monitors, oldsucc2)

		if newsucc2 != oldsucc2 {
			monitor_node := createMonitor(newsucc2)
			monitors["succ2"] = &monitor_node
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

	glog.Infof("old: %v", old_monitors)
	glog.Infof("new: %v", new_monitors)

	if !reflect.DeepEqual(old_monitors, new_monitors) {
		glog.Infof("[HEARTBEAT %d]Updated monitors from %v to %v", myVid, old_monitors, new_monitors)
	}
}


func garbageCollection() {
	for {
		time.Sleep(30 * time.Second)

		for i:=1; i<=maxID; i++ {
			mnode, isavailable := memberMap[i]
			if ((!isavailable || !mnode.alive) && (time.Now().Unix() - eventTimeMap[i] > 6)) {
				garbage[i] = true
			}
		}
		glog.Infof("[Introducer] Garbage set: %v", garbage)
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
		maxID = max(maxID, subject)

		glog.Info(message)

		switch message_type {
		case "ADD":
			var cnode ChildNode
			cnode.timestamp = time.Now().Unix()
			children[subject] = &cnode

			glog.Infof("[ADD %d] %d %d", myVid, subject, children[subject].timestamp)
			
			var newnode MemberNode
			newnode = createMember(split_message[2], split_message[3])
			memberMap[subject] = &newnode

		case "REMOVE":
			_, ok := children[subject]
			if ok {
				delete(children, subject)
			}
			glog.Infof("[REMOVE %d] %d", myVid, subject)

		case "INTRODUCER":
			if myVid == 0 {
				if len(memberMap) < 5 { // Handles 3 failure
					newnode := createMember(split_message[2],split_message[3])
					memberMap[subject] = &newnode

					tempmax, _ := strconv.Atoi(split_message[4])
					maxID = max(maxID, tempmax)

					glog.Infof("[INTRODUCER %d] Received an introducer message from %d", 0, subject)
					
					message := fmt.Sprintf("JOIN,%d,%s,%d", 0, memberMap[0].ip,memberMap[0].timestamp)

					// monitor_node := createMonitor(subject)
					// if initMessageCount == 0 {
					// 	monitors["pred"] = &monitor_node
					// 	monitors["succ1"] = &monitor_node
					// 	monitors["succ2"] = &monitor_node
					// 	message = fmt.Sprintf("ADD,%d,%s,%d", 0, memberMap[0].ip, memberMap[0].timestamp)
					// 	sendMessage(subject, message)
					// 	initMessageCount = initMessageCount + 1
					// } else{
					// 	updateMonitors()
					// }

					updateMonitors()
					eventTimeMap[0] = memberMap[0].timestamp
					disseminate(message)
				}
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

			// if initRun {
			// 	go updateFingerTable()
			// 	initRun = false
			// }
			go checkIntroducer()

		case "MEMBER":
			if subject == myVid {
				break
			}
			newnode := createMember(split_message[2], split_message[3])
			memberMap[subject] = &newnode

			updateMonitors() // as new entry comes along we need to modify the Monitor list

		case "JOIN":
			origin_time, _ := strconv.ParseInt(string(split_message[3]), 10, 64)

			_, ok := eventTimeMap[subject]
			if (!ok || eventTimeMap[subject] < origin_time) {
				eventTimeMap[subject] = origin_time
				disseminate(message)

				if subject != myVid {
					newnode := createMember(split_message[2], split_message[3])
					memberMap[subject] = &newnode
				}

				message := fmt.Sprintf("MEMBER,%d,%s,%d", myVid, myIP, memberMap[myVid].timestamp)
				sendMessage(subject, message)

				updateMonitors()
			} 

		case "LEAVE", "CRASH":
			// mark crashed

			// check if it is currently marked alive or not
			glog.Infof("Received CRASH for %d",subject)

			origin_time, _ := strconv.ParseInt(string(split_message[2]), 10, 64)

			_, ok := eventTimeMap[subject]
			if (!ok || eventTimeMap[subject] < origin_time){
				eventTimeMap[subject] = origin_time
				disseminate(message)

				_, ok := memberMap[subject]
				if ok {
					memberMap[subject].alive = false
					
					_, ok = children[subject]
					if ok {
						delete(children, subject)
					}
					updateMonitors()
				}
			}

			// Read the sender of the message
			// len_msg := len(split_message)
			

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

	myIP = getmyIP()
	glog.Info(myIP)

	if myIP == introducer {
		myVid = 0
		var node MemberNode
		node.ip = myIP
		node.timestamp = time.Now().Unix()
		node.alive = true
		memberMap[0] = &node
	}

	go listenOtherPort()

	go sendHeartbeat()
	go receiveHeartbeat()

	if myIP == introducer {
		// there should be a delay here - depending on how frequently the introducer is being pinged
		// if the system already exists in some form, the introducer shouldn't accept join requests until it knows what the maxID is 
		time.Sleep(time.Duration(introPingPeriod) * time.Second)

		go completeJoinRequests()
		go garbageCollection()
		// this garbage collection can occur concurrent to the addToDead list

	} else{
		time.Sleep(time.Duration(introPingPeriod) * time.Second)
		sendJoinRequest()
	}

	go checkChildren()
	go updateFingerTable()


	wg.Wait()
	return
}





