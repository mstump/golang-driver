golang-driver
=============

Golang wrapper of the DataStax/Cassandra [C++ driver](https://github.com/datastax/cpp-driver)

Basic support for prepared statements and ad hoc queries. Lacking support for collections, but that will be remedied shortly.

### Build

1. Clone the repo, and change to the directory.
1. Set GOPATH to local dir ```export GOPATH=`pwd` ```
1. Build and install the library ```go build cassandra && go install cassandra```
1. Build and run the example ```go build examples && ./examples```

### Example Usage

```golang
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
```
