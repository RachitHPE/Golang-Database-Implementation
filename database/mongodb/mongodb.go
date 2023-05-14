package mongodb

import (
	"context"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// creating the mongo client.
func createMongoDBClient(uri string) (*mongo.Client, context.Context, context.CancelFunc, error) {

	ctx, cancel := context.WithTimeout(context.Background(),
		30*time.Second)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	return client, ctx, cancel, err
}

func insertOneItem(client *mongo.Client, ctx context.Context) error {
	var document interface{}

	document = bson.D{
		{"rollNo", 175},
		{"maths", 80},
		{"science", 90},
		{"computer", 95},
	}

	// tables in mysql is equivalent to collections in nosql
	// Here database is test and collection name is dummyCollection.
	collection := client.Database("test").Collection("dummyCollection")

	result, err := collection.InsertOne(ctx, document)

	// handle the error
	if err != nil {
		panic(err)
	}

	// print the insertion id of the document,
	// if it is inserted.
	fmt.Println("Result of InsertOne")
	fmt.Println(result.InsertedID)

	return nil
}

func insertMultipleItem(client *mongo.Client, ctx context.Context) error {
	var documents []interface{}

	// Storing into interface list.
	documents = []interface{}{
		bson.D{
			{"rollNo", 153},
			{"maths", 65},
			{"science", 59},
			{"computer", 55},
		},
		bson.D{
			{"rollNo", 162},
			{"maths", 86},
			{"science", 80},
			{"computer", 69},
		},
	}

	// Here database is test and collection name is dummyCollection.
	collection := client.Database("test").Collection("dummyCollection")

	result, err := collection.InsertMany(ctx, documents)

	// handle the error
	if err != nil {
		panic(err)
	}

	fmt.Println("Result of InsertMany")

	for id := range result.InsertedIDs {
		fmt.Println(id)
	}

	return nil
}

func retrieveDocumentFromCollection(client *mongo.Client, ctx context.Context) error {
	collection := client.Database("test").Collection("dummyCollection")
	var filter, option interface{}

	// fetch those documents(equivalent to rows in mysql) where "maths" marks more than 70.
	filter = bson.D{
		{"maths", bson.D{{"$gt", 70}}},
	}

	//  option remove id field from all documents
	option = bson.D{{"_id", 0}}

	cursor, err := collection.Find(ctx, filter, options.Find().SetProjection(option))
	if err != nil {
		return err
	}

	var results []bson.D

	// to get bson object  from cursor,
	// returns error if any.
	if err := cursor.All(ctx, &results); err != nil {

		// handle the error
		return err
	}

	// printing the result of query.
	fmt.Println("Query Result")
	for _, doc := range results {
		fmt.Println(doc)
	}

	return nil
}

func updateDocumentInCollection(client *mongo.Client, ctx context.Context) error {
	collection := client.Database("test").Collection("dummyCollection")

	// if "maths" marks is less than 100, set it to 100.

	filter := bson.D{
		{"maths", bson.D{{"$lt", 100}}},
	}

	// The field of the document that need to updated.
	update := bson.D{
		{"$set", bson.D{
			{"maths", 100},
		}},
	}

	result, err := collection.UpdateMany(ctx, filter, update)
	if err != nil {
		panic(err)
	}

	// print count of documents that affected
	fmt.Println("updated document")
	fmt.Println(result.ModifiedCount)

	return nil
}

// close the mongodb client connection
func close(client *mongo.Client, ctx context.Context,
	cancel context.CancelFunc) {

	defer cancel()

	defer func() {
		if err := client.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()
}

func MongoDBInitialization() {

	// get Client, Context, CancelFunc and err from connect method.
	client, ctx, cancel, err := createMongoDBClient("mongodb://localhost:27017")
	if err != nil {
		panic(err)
	}

	// Release resource when main function is returned.
	defer close(client, ctx, cancel)

	err = insertOneItem(client, ctx)
	if err != nil {
		fmt.Println("failed inserting one item with error ", err)
	}

	err = insertMultipleItem(client, ctx)
	if err != nil {
		fmt.Println("failed inserting multiple item with error ", err)
	}

	err = retrieveDocumentFromCollection(client, ctx)
	if err != nil {
		fmt.Println("failed inserting one item with error ", err)
	}

	err = updateDocumentInCollection(client, ctx)
	if err != nil {
		fmt.Println("failed inserting one item with error ", err)
	}
}
