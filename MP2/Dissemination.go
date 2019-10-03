package main

import (
	"fmt"

)

type Node struct {
    IP string
    count  int64
    vid int64
}

var heartbeatStatus = make(map[string] Node)
func main() {
	
	val := Node{IP: "127.0.0.1", count : 0, vid: 0 }
	heartbeatStatus["A"] = val
	val2 := Node{IP: "127.0.0.2", count : 1, vid: 1 }
	heartbeatStatus["B"] = val2

	for k,v := range heartbeatStatus{
		fmt.Println(k,v.IP,v.count,v.vid)
	}
}
