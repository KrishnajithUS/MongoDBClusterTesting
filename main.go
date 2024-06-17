package main

import (
	"log"
	// "test/clustered_test"
	"test/non_clustered_test"
)

func main(){
	log.Print("------ Starting non clustered query ------")
	non_clustered_test.RunNonClustered()
	// log.Print("------ Starting clustered query ------")
	// clustered_test.RunClustered()
	
}