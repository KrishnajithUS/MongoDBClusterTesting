package fetch_operations

import (
	"context"
	"fmt"
	"log"
	"test/queries"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)






func FetchClustered() {

	var uri string

	uri = "mongodb://sa:Password123@127.0.10.1:27017,127.0.10.2:27017,127.0.10.3:27017/?replicaSet=rs0"
	fmt.Println(uri)

	clientOpts := options.Client().ApplyURI(uri)
	
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

	advertisementHistory := DB.Collection("Imported")
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	


	// log.Printf("------ Mongo Unordered Read ------")
	// startRead := time.Now()
	// queries.MongoReadClustered(advertisementHistory, ctx)
	// elapsedRead := time.Since(startRead).Seconds()
	// log.Printf("MongoRead took %f", elapsedRead)

	// log.Printf("------ Mongo Ordered Read ------")
	// startRead = time.Now()
	// queries.MongoReadSortByIDClustered(advertisementHistory, ctx)
	// elapsedRead = time.Since(startRead).Seconds()
	// log.Printf("MongoRead Sort By Id %f", elapsedRead)



	log.Printf("------ Update Device Id ------")
	startRead := time.Now()
	updatedFields := bson.D{{"deviceId", int64(18)},{"tMsgRecvByServer",int64(555777)},}
	queries.MongoUpdateDeviceId(advertisementHistory, ctx, updatedFields)
	elapsedRead := time.Since(startRead).Seconds()

	log.Printf("Updation took %f", elapsedRead)


	log.Printf("------ Mongo Read by Device Id ------")
	startRead = time.Now()
	queries.MongoReadByDevice(advertisementHistory, ctx, 18)
	elapsedRead = time.Since(startRead).Seconds()
	log.Printf("MongoRead Sort By Device took %f", elapsedRead)


	log.Printf("------ Update Audio Played ------")
	updatedFields = bson.D{{"audioPlayed", 88},}
	startRead = time.Now()
    queries.MongoUpdateaudioPlayed(advertisementHistory, ctx, 18, 555777,updatedFields)
	elapsedRead = time.Since(startRead).Seconds()
	log.Printf("Mongo Update Autido Played took %f", elapsedRead)




}
