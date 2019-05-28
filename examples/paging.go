package main

import (
	"fmt"
	"golang-driver/cassandra"
)

func main() {
	cluster := cassandra.NewCluster()
	cluster.SetContactPoints("127.0.0.1")
	defer cluster.Finalize()

	session := cassandra.NewSession()
	defer session.Finalize()

	sessfuture := cluster.SessionConnect(session)
	sessfuture.Wait()
	defer sessfuture.Finalize()

	statement := cassandra.NewStatement("SELECT id FROM table", 0)
	statement.SetPageSize(5000)
	defer statement.Finalize()

	var counter = 0
	hasMorePageSize := true

	for hasMorePageSize {
		stmtfuture := session.Execute(statement)
		stmtfuture.Wait()
		result := stmtfuture.Result()
		for result.Next() {
			var id int32
			result.Scan(&id)
			counter++
		}

		hasMorePageSize = result.HasMorePages()

		if (hasMorePageSize) {
			statement.SetPaginState(result)
		}
		stmtfuture.Finalize()
		result.Finalize()
	}

	fmt.Printf("DONE %d.\r\n", counter)
}
