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
var memberMap = make(map[int]MemberNode)
var heartbeatPort = "8080"
var myVid	// this will be populated after join by introducer
// var heartbeatconn *net.UDPConn
var heartbeatPeriod = 2	// in seconds
var children = make(map[string]ChildNode)
var suspects []int 	// remove from suspects when leave or crash 
var otherPort = "8081"
var delimiter = ","
// var otherportconn *net.UDPConn

func getMonitor(vid int) (MonitorNode, err error) {
	var node MonitorNode
	node.vid = vid
	node.ip = memberMap[vid].ip
	node.conn, err = net.DialUDP("udp", nil, node.ip + heartbeatPort)
	if err != nil {
		glog.Warning("Cannot setup a UDP connection with monitor vid=%d ip=%s", node.vid, node.ip)
		return err
	}
	return node
}

func setupMonitors() {
	n := len(memberMap)
	pred := (myVid - 1) % n
	succ1 := (myVid + 1) % n
	succ2 := (myVid + 2) % n

	for i, vid := range([]int{pred, succ1, succ2}) {
		node, err = getMonitor(vid)
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
		}
		time.Sleep(heartbeatPeriod * time.Second)
	}
}

func receiveHeartbeat() {

	udpAddress, err := net.ResolveUDPAddr("udp", heartbeatPort)
	if err != nil {
		glog.Warning("Unable to resolve my UDP address")
		return err
	}
	heartbeatconn, err = net.ListenUDP("udp", udpAddress)
	if err != nil {
		glog.Error("Unable to listen on the heartbeat port %s", heartbeatPort)
		return err
	}

	for {
		var buf [4]byte
		n, addr, err := heartbeatconn.ReadFromUDP(buf[0:])
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
	conn, err := net.DialUDP("udp", nil, memberMap[vid].ip + otherPort)
	if err != nil {
		glog.Warning("Unable to send message to vid=%d ip=%s", vid, memberMap[vid].ip)
	}
	defer conn.Close()
	conn.Write(message)
	return
}

func sendMessageAddr(addr string, message string) {
	conn, err := net.DialUDP("udp", nil, addr + otherPort)
	if err != nil {
		glog.Warning("Unable to send message to vid=%d ip=%s", vid, memberMap[vid].ip)
	}
	defer conn.Close()
	conn.Write(message)
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
	time.Sleep(0.5 * time.Second)
	for _, suspect := range(suspects) {
		if suspect == vid {
			// if it is still in the suspects list, mark as crashed and disseminate crash messages
		}
	}
	return
}

func massMail (message) {
	for vid, node := range(memberMap) {
		if node.alive == false {
			continue
		}
		sendMessage(vid, message)
	}
}

func listenOtherPort() {
	// setup other port
	udpAddress, err := net.ResolveUDPAddr("udp", otherPort)
	if err != nil {
		glog.Warning("Unable to resolve my UDP address")
		return err
	}
	otherportconn, err = net.ListenUDP("udp", udpAddress)
	if err != nil {
		glog.Error("Unable to listen on the heartbeat port %s", otherPort)
		return err
	}

	for {
		var buf [64]byte
		n, addr, err := otherportconn.ReadFromUDP(buf[0:])
		if err != nil {
			glog.Warning("Could not read  message on otherport %s", otherport)
		}
		message := string(buf[0:n])
		split_message := strings.Split(message, delimiter)
		message_type = split_message[0]
		subject = int(split_message[1])	// [todo]str to int Atoi

		switch message_type {
		case "JOIN":

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
			sendMessageAddr(addr, message)
		}
	case "STATUS":
		status = split_message[2]
		if status == 1 {
			// declare node as crashed
			MemberNode[subject].alive = false
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





