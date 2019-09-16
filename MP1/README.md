1. Clone the repository \
	(SSH) `git clone git@gitlab.engr.illinois.edu:dipayan2/cs425.git` \
	(HTTPS) `git clone https://gitlab.engr.illinois.edu/dipayan2/cs425.git` \

2. Install the golang color toolkit. This is required for the -visual boolean flag for highlighting matches for terminal output
	`go get github.com/gookit/color`

3. change directory to MP1
	`cd MP1`

4. Run the servers using
	`go run server.go`

5. Client takes in multiple commandline flag \
    **server_file**: path to the file containing server IPs and index, default is servers.in \
    **pattern**: regexp pattern to match, default is ^[0-9]*[a-z]{5} \
    **file_prefix**: prefix of the files before <i>.log, default is vm \
    Example, the prefix for vm<i>.log is "vm" and for machine.<i>.log is "machine." \
    **visual**: boolean flag, when set, prints annotated matches to the terminal and highlights patterns. github.com/gookit/color is used for the colored highlighting. \
    when false, the each server output is stored separately in 'filtered-<file_prefix><i>.log', default is false

	`go run client.go -server_file=servers.in -pattern=GET -file_prefix=vm -visual=false`
	
6. Unit testing \
    To run the test servers, `go run testServer.go` \
    To run the test client, `go run testClient.go -server_file=servers.in -file_prefix=testvm -visual=false`
   