package main

import (
  "fmt"
)

func main() {
  fmt.Println("This will happen first")

  go func() {
    fmt.Println("This will happen at some unknown time")
  }()

  fmt.Println("This will either happen second or third")

  fmt.Scanln()
  fmt.Println("done")
}