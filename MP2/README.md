1. Clone the repository \
	(SSH) `git clone git@gitlab.engr.illinois.edu:dipayan2/cs425.git` \
	(HTTPS) `git clone https://gitlab.engr.illinois.edu/dipayan2/cs425.git` 

2. change directory to MP2 \
	`cd MP2`

3. Run the membership.go file and specify a logfile path.
    For example, at VM1 \
	`go run membership.go -logfile=../MP1/mp2-vm1.log` \
    Note - Specify a logfile path inside MP1 to use distributed grep for debugging. 

4. To debug a certain event, say crash of a node \
    change directory to MP1 and run the `server.go` file at every node in the system \
    `cd MP1` \
    `go run server.go` 
    
    Now run the client at one node and query for the desired pattern \
    `go run client.go -server_file=servers.in -pattern=CRASH -file_prefix=mp2-vm -visual`
   