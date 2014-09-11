package main

import (
	"cassandra"
	"fmt"
)

func main() {
	cluster := cassandra.NewCassCluster()
	cluster.SetContactPoints("127.0.0.1")

	sessionFuture := cluster.Connect()
	sessionFuture.Wait()
	session := sessionFuture.Session()

	statement := cassandra.NewCassStatement("select cluster_name from system.local;", 0)
	future := session.Execute(statement)
	future.Wait()

	result := future.Result()

	fmt.Printf("Clusters:\r\n")
	for result.Next() {
		var clusterName string
		result.Scan(&clusterName)
		fmt.Printf("%s\n", clusterName)
	}

	fmt.Printf("DONE.\r\n")
}
