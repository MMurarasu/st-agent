package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gocql/gocql"
	"github.com/gorilla/mux"
)

type HostCheck struct {
	Token     string `json:"token"`
	Hostname  string `json:"hostname"`
	Date      string `json:"date"`
	Nextcheck int    `json:"nextcheck"`
}
type HostStatus struct {
	Hostname string `json:"hostname"`
	Status   string `json:"status"`
}

var (
	CassandraCon string
)

func main() {
	var listenPort = flag.String("port", "18080", "This is the port the application will listen on")
	flag.StringVar(&CassandraCon, "db", "database", "Cassandra Database address")
	flag.Parse()

	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/agent", createUpdateHost).Methods("POST")
	router.HandleFunc("/agent/{token}", getHosts).Methods("GET")
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", *listenPort), router))
}

func createUpdateHost(w http.ResponseWriter, r *http.Request) {
	var host HostCheck
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&host)
	if err != nil {
		fmt.Println(err)
	}

	cluster := gocql.NewCluster(CassandraCon)
	cluster.Keyspace = "status"
	cluster.Consistency = gocql.Quorum
	session, _ := cluster.CreateSession()
	defer session.Close()

	if err := session.Query(`INSERT INTO hosts (authtok, hostname, date, nextcheck) VALUES (?, ?, ?, ?)`, host.Token, host.Hostname, strconv.FormatInt(time.Now().Unix(), 10), host.Nextcheck).Exec(); err != nil {
		log.Fatal(err)
	}
}

func getHosts(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	cluster := gocql.NewCluster(CassandraCon)
	cluster.Keyspace = "status"
	cluster.Consistency = gocql.Quorum
	session, _ := cluster.CreateSession()
	defer session.Close()

	var hosts []HostStatus
	var hostname string
	var date string
	var nextcheck int

	iter := session.Query(`SELECT hostname, date, nextcheck FROM hosts WHERE authtok = ?`, vars["token"]).Iter()

	for iter.Scan(&hostname, &date, &nextcheck) {
		idate, err := strconv.Atoi(date)
		if int64(idate+nextcheck) < time.Now().Unix() {
			hosts = append(hosts, HostStatus{Hostname: hostname, Status: "DOWN"})
		} else {
			hosts = append(hosts, HostStatus{Hostname: hostname, Status: "OK"})
		}
		if err != nil {
			fmt.Println(err)
		}
	}
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(hosts)
	fmt.Println(vars["token"])
}
