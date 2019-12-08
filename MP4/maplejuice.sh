ulimit -n 4096
ulimit -a
go run membership.go main.go maplejuice.go helper.go --logfile=logs.txt