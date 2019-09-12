package main

import (
    "bufio"
    "fmt"
    "io"
    "os"
    "flag"
    "regexp"
    "github.com/gookit/color"
)

func grepOnFile(filepath string, pattern string) (err error) {
    r, _ := regexp.Compile(pattern)
    file, err := os.Open(filepath)
    defer file.Close()

    if err != nil {
        return err
    }

    magenta := color.FgMagenta.Render
    bold := color.OpBold.Render

    reader := bufio.NewReader(file)
    var line string
    var linenum int = 0

    for {
        line, err = reader.ReadString('\n')
        var idx_arr [][]int

        if r.MatchString(line) {
            fmt.Printf("%d: ", linenum)
            idx_arr = r.FindAllStringIndex(line, -1)
            var i int = 0
            for _, element := range idx_arr {
                fmt.Printf("%s %s", line[i:element[0]], bold(magenta(line[element[0]:element[1]])))
                i = element[1]
            }
            fmt.Printf("%s", line[i:])
        }
        linenum += 1
        if err != nil {
            break
        }
    }
    fmt.Println()

    if err != io.EOF {
        fmt.Printf(" > Failed!: %v\n", err)
    }
    return
}


func main(){
    filepath := flag.String("filepath", "test.out", "path to the file")
    pattern := flag.String("pattern", "research", "pattern to match")

    flag.Parse()

    grepOnFile(*filepath, *pattern)  
}