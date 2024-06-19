package main

import (
	"log"
	"os"
	"test/clustered_test"
	"test/non_clustered_test"
)

func main() {
	testType := os.Args
	if len(testType) >0 && testType[1] == "c" {
		log.Print("------ Starting clustered query ------")
		clustered_test.RunClustered()

	} else {
		log.Print("------ Starting non clustered query ------")
		non_clustered_test.RunNonClustered()
	}

}
