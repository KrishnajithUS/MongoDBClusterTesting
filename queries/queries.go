package queries

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var globalCounter int64 = 0
var maxRecord int64 = 10000

type AdvertisementHistoryMDB struct {
	ID                   primitive.ObjectID `bson:"_id,omitempty"`
	RequestRefNo         string             `bson:"reqRefNo" json:"reqRefNo" binding:"required"`
	RRN                  string             `bson:"rrn" json:"rrn"`
	MerchantId           string             `bson:"-" json:"mid"`
	TerminalID           string             `bson:"-" json:"tid,omitempty"`
	MerchantCategoryCode string             `bson:"-" json:"mcc"`
	MerchantVPA          string             `bson:"-" json:"meVpa"`
	TransactionType      int8               `bson:"transactionType" json:"transactionType"`
	TransactionMode      int8               `bson:"transactionMode" json:"transactionMode"`
	Amount               string             `bson:"txnAmt" json:"txnAmt" binding:"required"`
	TransactionTimeStamp string             `bson:"txnTimeStamp" json:"txnTimestamp"`
	TimeStamp            int64              `bson:"timeStamp" json:"timeStamp"`
	DeviceID             int64              `bson:"deviceId" json:"deviceId"`
	ExpirationTime       int64              `bson:"expirationTime" json:"-"`
	TMsgRecvByServer     int64              `bson:"tMsgRecvByServer" json:"tMsgRecvByServer"`
	TMsgRecvFromDevice   int64              `bson:"tMsgRecvFromDev" json:"tMsgRecvFromDev"`
	AudioPlayed          uint8              `bson:"audioPlayed" json:"audioPlayed"`
	MessageId            int64              `gorm:"-" json:"id"`
}

type AdvertisementHistoryMDBClustered struct {
	ID                   primitive.ObjectID `bson:"_id,omitempty"`
	RequestRefNo         string             `bson:"reqRefNo" json:"reqRefNo" binding:"required"`
	RRN                  string             `bson:"rrn" json:"rrn"`
	MerchantId           string             `bson:"-" json:"mid"`
	TerminalID           string             `bson:"-" json:"tid,omitempty"`
	MerchantCategoryCode string             `bson:"-" json:"mcc"`
	MerchantVPA          string             `bson:"-" json:"meVpa"`
	TransactionType      int8               `bson:"transactionType" json:"transactionType"`
	TransactionMode      int8               `bson:"transactionMode" json:"transactionMode"`
	Amount               string             `bson:"txnAmt" json:"txnAmt" binding:"required"`
	TransactionTimeStamp string             `bson:"txnTimeStamp" json:"txnTimestamp"`
	TimeStamp            int64              `bson:"timeStamp" json:"timeStamp"`
	DeviceID             int64              `bson:"deviceId" json:"deviceId"`
	ExpirationTime       int64              `bson:"expirationTime" json:"-"`
	TMsgRecvByServer     int64              `bson:"tMsgRecvByServer" json:"tMsgRecvByServer"`
	TMsgRecvFromDevice   int64              `bson:"tMsgRecvFromDev" json:"tMsgRecvFromDev"`
	AudioPlayed          uint8              `bson:"audioPlayed" json:"audioPlayed"`
	MessageId            int64              `gorm:"-" json:"id"`
}

type MetaData struct {
	DeviceID     int64  `bson:"deviceId" json:"deviceId"`
	RequestRefNo string `bson:"reqRefNo" json:"reqRefNo"`
}

type AdvertisementHistoryMDBTimeSeries struct {
	ID                 primitive.ObjectID `bson:"_id,omitempty"`
	AdvertisementID    int64              `bson:"addId" json:"addId" binding:"required"`
	TerminalID         string             `bson:"-" json:"tid,omitempty"`
	TimeStamp          int64              `bson:"timeStamp" json:"timeStamp"`
	ExpirationTime     int64              `bson:"expirationTime" json:"-"`
	TMsgRecvByServer   int64              `bson:"tMsgRecvByServer" json:"tMsgRecvByServer"`
	TMsgRecvFromDevice int64              `bson:"tMsgRecvFromDev" json:"tMsgRecvFromDev"`
	AudioPlayed        uint8              `bson:"audioPlayed" json:"audioPlayed"`
	CreatedBy          int64              `bson:"createdBy" json:"-"`
	Meta               MetaData           `bson:"meta"`
}

func CreateIndex(collection *mongo.Collection, ctx context.Context) {
	indexes := []mongo.IndexModel{
		{Keys: bson.D{{"deviceId", 1}, {"audioPlayed", 1}}},
		{Keys: bson.D{{"deviceId", 1}, {"tMsgRecvByServer", 1}}},
		{Keys: bson.D{{"reqRefNo", 1}}},
		{Keys: bson.D{{"tMsgRecvByServer", 1}, {"audioPlayed", 1}}},
	}
	names, err := collection.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		log.Fatalf("Failed to create indexes: %v", err)
	}
	for _, name := range names {
		fmt.Println("Created Index: " + name)
	}
}

func dropAllIndex(collection *mongo.Collection, ctx context.Context) {
	indexes, err := collection.Indexes().DropAll(ctx)
	if err != nil {
		log.Fatalf("Failed to drop indexes: %v", err)
	}
	fmt.Println("Dropped Indexes: " + string(indexes))
}

func MongoWriteClustered(collection *mongo.Collection, ctx context.Context) {
	globalCounter++
	doc := AdvertisementHistoryMDBClustered{
		RequestRefNo:       fmt.Sprintf("REQ%d", globalCounter),
		DeviceID:           int64(globalCounter),
		TimeStamp:          time.Now().Unix(),
		ExpirationTime:     time.Now().Add(24 * time.Hour).Unix(),
		TMsgRecvByServer:   time.Now().Add(1 * time.Hour).Unix(),
		TMsgRecvFromDevice: time.Now().Add(2 * time.Hour).Unix(),
		AudioPlayed:        uint8(globalCounter % 2),
		Amount:             "4000.00",
		TransactionType:    3,
		RRN:                "ddff",
	}

	_, err := collection.InsertOne(ctx, doc)
	if err != nil {
		log.Fatalf("Failed to insert document: %v", err)
	}
}

func MongoReadClustered(collection *mongo.Collection, ctx context.Context) {
	var dataAdv []AdvertisementHistoryMDBClustered
	opt := options.Find().SetLimit(maxRecord)
	dataAdvCursor, err := collection.Find(ctx, bson.D{}, opt)
	if err != nil {
		log.Fatalf("Failed to find documents: %v", err)
	}
	defer dataAdvCursor.Close(ctx)

	if err = dataAdvCursor.All(ctx, &dataAdv); err != nil {
		log.Fatalf("Failed to decode documents: %v", err)
	}
}

func MongoWrite(collection *mongo.Collection, ctx context.Context) {
	globalCounter++
	doc := AdvertisementHistoryMDB{
		RequestRefNo:       fmt.Sprintf("REQ%d", globalCounter),
		DeviceID:           int64(globalCounter),
		TimeStamp:          time.Now().Unix(),
		ExpirationTime:     time.Now().Add(24 * time.Hour).Unix(),
		TMsgRecvByServer:   time.Now().Add(1 * time.Hour).Unix(),
		TMsgRecvFromDevice: time.Now().Add(2 * time.Hour).Unix(),
		AudioPlayed:        uint8(globalCounter % 2),
		Amount:             "4000.00",
		TransactionType:    3,
		RRN:                "ddff",
	}

	_, err := collection.InsertOne(ctx, doc)
	if err != nil {
		log.Fatalf("Failed to insert document: %v", err)
	}
}

func MongoTimeSeries(collection *mongo.Collection, ctx context.Context) {
	var documents []interface{}

	for i := 0; i < 10; i++ {
		globalCounter++
		doc := AdvertisementHistoryMDBTimeSeries{
			AdvertisementID:    int64(globalCounter + 1000),
			TimeStamp:          time.Now().Unix(),
			ExpirationTime:     time.Now().Add(24 * time.Hour).Unix(),
			TMsgRecvByServer:   time.Now().Add(1 * time.Hour).Unix(),
			TMsgRecvFromDevice: time.Now().Add(2 * time.Hour).Unix(),
			AudioPlayed:        uint8(globalCounter % 2),
			CreatedBy:          int64(globalCounter + 100),
			Meta: MetaData{
				DeviceID:     int64(globalCounter),
				RequestRefNo: fmt.Sprintf("REQ%d", i),
			},
		}
		documents = append(documents, doc)
	}

	_, err := collection.InsertMany(ctx, documents)
	if err != nil {
		log.Fatalf("Failed to insert documents: %v", err)
	}

	fmt.Println("Documents inserted successfully")
}

func MongoRead(collection *mongo.Collection, ctx context.Context) {
	var dataAdv []AdvertisementHistoryMDB
	opt := options.Find().SetLimit(maxRecord)
	dataAdvCursor, err := collection.Find(ctx, bson.D{}, opt)
	if err != nil {
		log.Fatalf("Failed to find documents: %v", err)
	}
	defer dataAdvCursor.Close(ctx)

	if err = dataAdvCursor.All(ctx, &dataAdv); err != nil {
		log.Fatalf("Failed to decode documents: %v", err)
	}
}

func MongoReadSortByID(collection *mongo.Collection, ctx context.Context) {
	var dataAdv []AdvertisementHistoryMDB
	opts := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}})
	opts.SetLimit(maxRecord)
	dataAdvCursor, err := collection.Find(ctx, bson.D{}, opts)
	if err != nil {
		log.Fatalf("Failed to find documents: %v", err)
	}
	defer dataAdvCursor.Close(ctx)

	if err = dataAdvCursor.All(ctx, &dataAdv); err != nil {
		log.Fatalf("Failed to decode documents: %v", err)
	}
}

func MongoReadSortByIDClustered(collection *mongo.Collection, ctx context.Context) {
	var dataAdv []AdvertisementHistoryMDBClustered
	opts := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}})
	opts.SetLimit(maxRecord)
	dataAdvCursor, err := collection.Find(ctx, bson.D{}, opts)
	if err != nil {
		log.Fatalf("Failed to find documents: %v", err)
	}
	defer dataAdvCursor.Close(ctx)

	if err = dataAdvCursor.All(ctx, &dataAdv); err != nil {
		log.Fatalf("Failed to decode documents: %v", err)
	}
}

func MongoReadSortByDeviceIdIndex(collection *mongo.Collection, ctx context.Context) {
	var dataAdv []AdvertisementHistoryMDB
	opts := options.Find().SetSort(bson.D{{Key: "deviceId", Value: 1}})
	opts.SetLimit(maxRecord)
	dataAdvCursor, err := collection.Find(ctx, bson.D{}, opts)
	if err != nil {
		log.Fatalf("Failed to find documents: %v", err)
	}
	defer dataAdvCursor.Close(ctx)

	if err = dataAdvCursor.All(ctx, &dataAdv); err != nil {
		log.Fatalf("Failed to decode documents: %v", err)
	}
}

func MongoReadSortByIndexClustered(collection *mongo.Collection, ctx context.Context) {
	var dataAdv []AdvertisementHistoryMDB
	opts := options.Find().SetSort(bson.D{{Key: "deviceId", Value: 1}})
	opts.SetLimit(maxRecord)
	dataAdvCursor, err := collection.Find(ctx, bson.D{}, opts)
	if err != nil {
		log.Fatalf("Failed to find documents: %v", err)
	}
	defer dataAdvCursor.Close(ctx)

	if err = dataAdvCursor.All(ctx, &dataAdv); err != nil {
		log.Fatalf("Failed to decode documents: %v", err)
	}
}

func GetAllIndexInCollection(db *mongo.Database, collection *mongo.Collection, ctx context.Context) {
	command := bson.D{{Key: "listIndexes", Value: "AdvertisementHistoryMDB"}}
	var result bson.M
	err := db.RunCommand(context.TODO(), command).Decode(&result)
	fmt.Println("GetAllIndexInCollection")
	if err != nil {
		log.Fatalf("Failed to get all indexes: %v", err)
	}
	fmt.Println("All Indexes", result)
}

func GetAllIndexInCollectionCluster(db *mongo.Database, collection *mongo.Collection, ctx context.Context) {
	command := bson.D{{Key: "listIndexes", Value: "AdvertisementHistoryMDBClustered"}}
	var result bson.M
	err := db.RunCommand(context.TODO(), command).Decode(&result)
	fmt.Println("GetAllIndexInCollection")
	if err != nil {
		log.Fatalf("Failed to get all indexes: %v", err)
	}
	fmt.Println("All Indexes", result)
}

func MongoReadByDeviceAndTimestamp(collection *mongo.Collection, ctx context.Context, deviceId int64, startTime, endTime int64) {
	var dataAdv []AdvertisementHistoryMDB
	filter := bson.D{
		{"deviceId", deviceId},
		{"timeStamp", bson.D{
			{"$gte", startTime},
			{"$lte", endTime},
		}},
	}

	dataAdvCursor, err := collection.Find(ctx, filter)
	if err != nil {
		log.Fatalf("Failed to find documents: %v", err)
	}
	defer dataAdvCursor.Close(ctx)

	if err = dataAdvCursor.All(ctx, &dataAdv); err != nil {
		log.Fatalf("Failed to decode documents: %v", err)
	}
}

func MongoReadClusteredByDeviceAndTimestamp(collection *mongo.Collection, ctx context.Context, deviceId int64, startTime, endTime int64) {
	var dataAdv []AdvertisementHistoryMDBClustered
	filter := bson.D{
		{"deviceId", deviceId},
		{"timeStamp", bson.D{
			{"$gte", startTime},
			{"$lte", endTime},
		}},
	}
	dataAdvCursor, err := collection.Find(ctx, filter)
	if err != nil {
		log.Fatalf("Failed to find documents: %v", err)
	}
	defer dataAdvCursor.Close(ctx)

	if err = dataAdvCursor.All(ctx, &dataAdv); err != nil {
		log.Fatalf("Failed to decode documents: %v", err)
	}
}

func MongoUpdateClusteredByDeviceAndTimestamp(collection *mongo.Collection, ctx context.Context, deviceId int64, startTime, endTime int64, updateFields bson.D) {
	filter := bson.D{
		{"deviceId", deviceId},
		{"timeStamp", bson.D{
			{"$gte", startTime},
			{"$lte", endTime},
		}},
	}
	update := bson.D{
		{"$set", updateFields},
	}
	result, err := collection.UpdateMany(ctx, filter, update)
	if err != nil {
		log.Fatalf("Failed to update documents: %v", err)
	}
	fmt.Printf("Matched %v documents and updated %v documents.\n", result.MatchedCount, result.ModifiedCount)
}

func GetIndexStatus(collection *mongo.Collection, ctx context.Context) {
	aggCommand := bson.D{
		{Key: "$indexStats", Value: bson.D{}},
	}
	cursor, err := collection.Aggregate(ctx, mongo.Pipeline{aggCommand})
	fmt.Println("GetIndexStatus")
	if err != nil {
		log.Fatalf("Failed to get index status: %v", err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var result bson.M
		if err := cursor.Decode(&result); err != nil {
			log.Fatalf("Failed to decode index status: %v", err)
		}
		fmt.Printf("%+v\n", result)
	}
	if err := cursor.Err(); err != nil {
		log.Fatalf("Cursor error: %v", err)
	}
}

// Returns a collection metrics on instance-wide
// resource utilization and status.
func GetServerStatus(db *mongo.Database, ctx context.Context) {
	var result bson.M
	command := bson.D{
		{Key: "serverStatus", Value: 1},
	}
	err := db.RunCommand(ctx, command).Decode(&result)
	fmt.Println("GetServerStatus")
	if err != nil {
		log.Fatalf("Failed to get server status: %v", err)
	}
	fmt.Printf("Server status: %+v\n", result)
}

func RunExplainOnCollection(db *mongo.Database, ctx context.Context) {
	var result bson.M
	// Create the explain command
	command := bson.D{
		{Key: "explain", Value: bson.D{
			{Key: "find", Value: "AdvertisementHistoryMDB"},
			{Key: "sort", Value: bson.D{{Key: "deviceId", Value: 1}}},
		}},
		{Key: "verbosity", Value: "executionStats"},
	}

	err := db.RunCommand(ctx, command).Decode(&result)
	fmt.Println("RunExplainOnCollection")
	if err != nil {
		log.Fatalf("Failed to run explain: %v", err)
	}
	fmt.Printf("Explain result: %+v\n", result)
}

func RunExplainOnCollectionCluster(db *mongo.Database, ctx context.Context) {
	var result bson.M
	// Create the explain command
	command := bson.D{
		{Key: "explain", Value: bson.D{
			{Key: "find", Value: "AdvertisementHistoryMDBClustered"},
			{Key: "sort", Value: bson.D{{Key: "deviceId", Value: 1}}},
		}},
		{Key: "verbosity", Value: "executionStats"},
	}

	err := db.RunCommand(ctx, command).Decode(&result)
	fmt.Println("RunExplainOnCollectionCluster")
	if err != nil {
		log.Fatalf("Failed to run explain: %v", err)
	}
	fmt.Printf("Explain result: %+v\n", result)
}
