package clustered_test

import (
	"context"
	"fmt"
	"log"
	"test/queries"
	"os"
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
	log.Println("Total Insertions:", insertCount*10)
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

func RunClustered() {
	
	lst :=[]int{100}
	if len(os.Args) < 1 {
		panic("Usage: program [readPreference]")
	}

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

	cio := bson.D{{"key", bson.D{{"_id", 1}}}, {"unique", true}}

	opts := options.CreateCollection().SetClusteredIndex(cio)

	DB.CreateCollection(context.TODO(), "AdvertisementHistoryMDBClustered", opts)
	advertisementHistory := DB.Collection("AdvertisementHistoryMDBClustered")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer logEvents(DB, advertisementHistory, ctx )
	
	for j := range lst{
		startWrite := time.Now()
		avg := 0.0
		for i := 0; i < lst[j]; i++ {
			singleTransactionStartTime := time.Now()
			queries.MongoWriteClustered(advertisementHistory, ctx)
			endOfTransaction := time.Since(singleTransactionStartTime).Seconds()
			avg += endOfTransaction
		}
		
		elapsedWrite := time.Since(startWrite).Seconds()
		insertionPerSecond := float64(lst[j])/elapsedWrite
		log.Printf("MongoWrite took %f for %d iterations", elapsedWrite, lst[j])
		log.Printf("Insertions Per Second %f", insertionPerSecond)
		log.Printf("Average Insertions %f", avg/float64(lst[j]))


		log.Printf("------ Mongo Unordered Read ------")
		startRead := time.Now()
		queries.MongoReadClustered(advertisementHistory, ctx)
		elapsedRead := time.Since(startRead)
		log.Printf("MongoRead took %s", elapsedRead)

		log.Printf("------ Mongo Ordered Read ------")
		startRead = time.Now()
		queries.MongoReadSortByIDClustered(advertisementHistory, ctx)
		elapsedRead = time.Since(startRead)
		log.Printf("MongoRead Sort By Id %s", elapsedRead)


		// Log the servers used for write and read operations
		log.Println("Write operations were performed on the following servers:")
		for server, count := range writeServers {
			log.Printf("Server: %s, Count: %d", server, count)
		}

		log.Println("Read operations were performed on the following servers:")
		for server, count := range readServers {
			log.Printf("Server: %s, Count: %d", server, count)
		}

}
	file, err := os.OpenFile("events.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file: %s", err)
	}
	defer file.Close()

	// Create a logger that writes to the file
	logger := log.New(file, "", log.LstdFlags)

	for i := range eventArray {
		logger.Println("Writing Events", eventArray[i])
	}
}
