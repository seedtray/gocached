package main

import (
	"flag"
	"os"
	"log"
	"net"
  /*"runtime"*/
  /*"runtime/pprof"*/
)

//global logger
var logger = log.New(os.Stdout, "gocached: ", log.Lshortfile|log.LstdFlags)

// specific typing for base storage factory, just build a map cache storage
func base_storage_factory () CacheStorage { return newMapCacheStorage() }

func main() {

  /*runtime.GOMAXPROCS(1)*/
	// command line flags and parsing
	var port = flag.String("port", "11212", "memcached port")
  /*var memprofile = flag.String("memprofile", "", "write memory profile to this file")*/

  var storage_choice = flag.String("storage", "generational",
		"storage implementation (generational, heap, leak)")
	var expiring_frequency = flag.Int64("expiring-interval", 10,
		"expiring interval in seconds")
  var partitions = flag.Int("partitions", 10,
    "storage partitions (0 or 1 to disable)")
	flag.Parse()


  /*if *memprofile != "" {*/
    /*defer func() {*/
      /*f, err := os.Create(*memprofile)*/
      /*if err != nil {*/
        /*log.Fatal(err)*/
      /*}*/
      /*pprof.WriteHeapProfile(f)*/
      /*f.Close()*/
    /*}()*/
  /*}*/

	// whether using partitioned or single storage

  var partition_storage CacheStorage
  var eventful_storage CacheStorage

	if *partitions > 1 {
    partition_storage = newHashingStorage(uint32(*partitions), base_storage_factory)
	} else {
		partition_storage = base_storage_factory()
	}

	// eventful storage implementation selection
	switch *storage_choice {
	case "leak":
		logger.Print("warning, will not expire entries")
    eventful_storage = partition_storage
	case "generational":
    updatesChannel := make(chan UpdateMessage, 5000)
    eventful_storage = newEventNotifierStorage(partition_storage, updatesChannel)
    newGenerationalStorage(*expiring_frequency, partition_storage, updatesChannel)
	case "heap":
    updatesChannel := make(chan UpdateMessage, 5000)
    eventful_storage = newEventNotifierStorage(partition_storage, updatesChannel)
    NewHeapExpiringStorage(*expiring_frequency, partition_storage, updatesChannel)
  }

	// network setup
	if addr, err := net.ResolveTCPAddr("tcp", "0.0.0.0:"+*port); err != nil {
		logger.Fatalf("Unable to resolv local port %s\n", *port)
	} else if listener, err := net.ListenTCP("tcp", addr); err != nil {
		logger.Fatalln("Unable to listen on requested port")
	} else {
    // server loop
    logger.Printf("Starting Gocached server")
    /*for i := 0; i < 21; i++ {*/
    for {
      if conn, err := listener.AcceptTCP(); err != nil {
        logger.Println("An error ocurred accepting a new connection")
      } else {
        go clientHandler(conn, eventful_storage)
      }
    }
  }
}

func clientHandler(conn *net.TCPConn, store CacheStorage) {
	defer conn.Close()
	if session, err := NewSession(conn, store); err != nil {
		logger.Println("An error ocurred creating a new session")
	} else {
    session.CommandLoop()
	}
}
