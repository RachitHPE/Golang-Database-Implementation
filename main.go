package main

import (
	"fmt"

	"hello/database/dynamodb"
	"hello/database/mongodb"
	"hello/database/mysql"

	_ "github.com/go-sql-driver/mysql"
)

func main() {

	fmt.Println("Enter the database u want to play with :) mysql | mongodb | dynamodb")
	var database string

	_, err := fmt.Scanf("%s", database)
	if err != nil {
		fmt.Printf("Failed taking input from user %v ", err)
	}

	switch database {
	case "mysql":
		mysql.MySQLInitialization()
	case "mongodb":
		mongodb.MongoDBInitialization()
	case "dynamodb":
		dynamodb.DynamodbInitialization()
	default:
		fmt.Println("Invalid Input given")

	}

}
