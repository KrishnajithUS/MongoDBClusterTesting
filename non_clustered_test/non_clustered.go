package non_clustered_test

import (
	"context"
	"fmt"
	"log"

	// "os"
	"test/queries"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var insertCount int = 0
var eventArray []*event.ServerDescriptionChangedEvent

var writeServers = make(map[string]int)
var readServers = make(map[string]int)

func logEvents(db *mongo.Database, collection *mongo.Collection, ctx context.Context) {
	log.Println("Total Insertions:", insertCount)
	queries.GetAllIndexInCollection(db, collection, ctx)
	// queries.GetIndexStatus(collection, ctx)
	// queries.GetServerStatus(db, ctx)
	// queries.RunExplainOnCollection(db, ctx)
}

var cmdMonitor *event.CommandMonitor = &event.CommandMonitor{
	Started: func(_ context.Context, evt *event.CommandStartedEvent) {
		if evt.CommandName == "insert" {
			writeServers[evt.ConnectionID]++
		}
		if evt.CommandName == "find" {
			readServers[evt.ConnectionID]++
		}
	},
	Succeeded: func(_ context.Context, evt *event.CommandSucceededEvent) {
		if evt.CommandName == "insert" {
			insertCount++
		}
	},
	Failed: func(_ context.Context, evt *event.CommandFailedEvent) {
	},
}

var srvMonitor *event.ServerMonitor = &event.ServerMonitor{
	ServerDescriptionChanged: func(e *event.ServerDescriptionChangedEvent) {
		eventArray = append(eventArray, e)
	},
}

func getReplicaSetStatus(client *mongo.Client, ctx context.Context) (primary string, secondaries []string, err error) {
	var result bson.M
	err = client.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetGetStatus", Value: 1}}).Decode(&result)
	if err != nil {
		return "", nil, err
	}

	members := result["members"].(bson.A)
	for _, member := range members {
		memberMap := member.(bson.M)
		stateStr := memberMap["stateStr"].(string)
		name := memberMap["name"].(string)
		if stateStr == "PRIMARY" {
			primary = name
		} else if stateStr == "SECONDARY" {
			secondaries = append(secondaries, name)
		}
	}
	return primary, secondaries, nil
}

func RunNonClustered() {

	lst := []int{1000000}

	var uri string

	uri = "mongodb://sa:Password123@127.0.10.1:27017,127.0.10.2:27017,127.0.10.3:27017/?replicaSet=rs0"
	fmt.Println(uri)

	clientOpts := options.Client().ApplyURI(uri).SetServerMonitor(srvMonitor).SetMonitor(cmdMonitor)

	client, err := mongo.Connect(context.TODO(), clientOpts)
	if err != nil {
		log.Println(err)
	}
	defer func() {
		if err = client.Disconnect(context.TODO()); err != nil {
			log.Fatal(err)
		}
	}()

	DB := client.Database("AdvHistory")

	advertisementHistory := DB.Collection("AdvertisementHistoryMDB")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer logEvents(DB, advertisementHistory, ctx)
	fmt.Println("Outside Main Loop", lst)
	// create index
	queries.CreateIndex(advertisementHistory, ctx)
	// for j := range lst {

	var singleTransactionStartTime time.Time
	var endOfTransaction float64

	startWrite := time.Now()
	avg := 0.0
	
	fmt.Println("Inside Main Loop", lst[0])
	for i := 0; i < lst[0]; i++ {
		singleTransactionStartTime = time.Now()
		queries.MongoWrite(advertisementHistory, ctx)
		endOfTransaction = time.Since(singleTransactionStartTime).Seconds()
		avg += endOfTransaction
	}
	elapsedWrite := time.Since(startWrite).Seconds()
	insertionPerSecond := float64(lst[0]) / elapsedWrite
	log.Printf("MongoWrite took %f for %d iterations", elapsedWrite, lst[0])
	log.Printf("Insertions Per Second %f", insertionPerSecond)
	log.Printf("Average Insertions %f", avg/float64(lst[0]))

	// log.Printf("------ Mongo Unordered Read ------")
	// startRead := time.Now()
	// queries.MongoRead(advertisementHistory, ctx)
	// elapsedRead := time.Since(startRead).Seconds()
	// log.Printf("MongoRead took %f", elapsedRead)
	// log.Printf("Reads Per Second %v", float64(j)/elapsedRead)

	// log.Printf("------ Mongo Ordered Read ------")
	// startRead = time.Now()
	// queries.MongoReadSortByID(advertisementHistory, ctx)
	// elapsedRead = time.Since(startRead).Seconds()
	// log.Printf("MongoRead Sort By Id %f", elapsedRead)
	// log.Printf("Reads Per Second %v", float64(j)/elapsedRead)

	// Log the servers used for write and read operations
	log.Println("Write operations were performed on the following servers:")
	for server, count := range writeServers {
		log.Printf("Server: %s, Count: %d", server, count)
	}

	log.Println("Read operations were performed on the following servers:")
	for server, count := range readServers {
		log.Printf("Server: %s, Count: %d", server, count)
	}

	// }
	// file, err := os.OpenFile("events.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	// if err != nil {
	// 	log.Fatalf("Failed to open log file: %s", err)
	// }
	// defer file.Close()

	// // Create a logger that writes to the file
	// logger := log.New(file, "", log.LstdFlags)

	// for i := range eventArray {
	// 	logger.Println("Writing Events", eventArray[i])
	// }
}
