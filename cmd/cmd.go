package cmd

import (
	"flag"

	"github.com/euforic/scds/dataserver"
)

func Start() {
	db := flag.String("db", ":memory:", "database connection string")
	port := flag.String("listen", ":9999", "web server host:port to listen on")
	flag.Parse()

	server := dataserver.New()
	server.Start(*db, *port)
}
