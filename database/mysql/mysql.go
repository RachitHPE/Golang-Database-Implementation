package mysql

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

type Tag struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

var db *sql.DB

func createTable(db *sql.DB) error {

	results, err := db.Prepare("create table tags(id varchar(50), name varchar(50))")
	if err != nil {
		fmt.Println("failed querying from database with error", err)
	}

	execute, err := results.Exec()
	if err != nil {
		fmt.Println("Error executing query ", err)
	}

	rowsAffected, err := execute.RowsAffected()
	if err != nil {
		fmt.Println("failed persisting rows with error", err)
	}

	fmt.Println("number of rows affected", rowsAffected)

	return nil
}

func insertIntoTable(db *sql.DB) error {
	prepareQuery, err := db.Prepare("INSERT INTO tags(id, name) VALUES (?, ?)")
	if err != nil {
		fmt.Println("Error preparing query ", err)
	}

	execute, err := prepareQuery.Exec("4", "TEST4")
	if err != nil {
		fmt.Println("Error executing query ", err)
	}

	rowsAffected, err := execute.RowsAffected()
	if err != nil {
		fmt.Println("failed persisting rows with error", err)
	}

	fmt.Println("number of rows affected", rowsAffected)

	return nil
}

func getRowsFromTable(db *sql.DB) error {

	results, err := db.Query("SELECT id, name FROM tags")
	if err != nil {
		fmt.Println("failed querying from database with error", err)
	}

	for results.Next() {
		var tag Tag
		// for each row, scan the result into our tag composite object
		err = results.Scan(&tag.ID, &tag.Name)
		if err != nil {
			fmt.Println("failed unmarshalling data with error ", err)
		}

		fmt.Println("id is ", tag.ID)
		fmt.Println("name is ", tag.Name)
	}

	return nil
}

func getById(db *sql.DB) error {

	var tag Tag
	// Execute the query
	err := db.QueryRow("SELECT id, name FROM tags where id = ?", 2).Scan(&tag.ID, &tag.Name)
	if err != nil {
		fmt.Println("failed querying from database with error", err)
	}

	fmt.Println("id is ", tag.ID)
	fmt.Println("name is ", tag.Name)

	return nil
}

func updateRowsInTable(db *sql.DB) error {
	stmt, err := db.Prepare("update tags set name=? where id=?")
	if err != nil {
		fmt.Println("failed preparing query with error", err)
	}

	// execute
	res, err := stmt.Exec("dummy", "1")
	if err != nil {
		fmt.Println("failed executing query with error", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		fmt.Println("failed persisting rows with error", err)
	}

	fmt.Println("number of rows affected", rowsAffected)

	return nil
}

func deleteRowsInTable(db *sql.DB) error {
	stmt, err := db.Prepare("delete from tags where id=?")
	if err != nil {
		fmt.Println("failed preparing query with error", err)
	}

	// execute
	res, err := stmt.Exec("4")
	if err != nil {
		fmt.Println("failed executing query with error", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		fmt.Println("failed persisting rows with error", err)
	}

	fmt.Println("number of rows affected", rowsAffected)

	return nil
}

func MySQLInitialization() {
	// mysql instance running on localhost:3306. Here database is testing
	db, err := sql.Open("mysql", "root:root@tcp(127.0.0.1:3306)/testing")

	if err != nil {
		panic(err.Error())
	}

	defer db.Close()

	// Let's first initialize the table
	err = createTable(db)
	if err != nil {
		fmt.Println("error encountered in creating table ", err)
	}

	// Let's insert data in the table
	err = insertIntoTable(db)
	if err != nil {
		fmt.Println("error inserting data in the table ", err)
	}

	// Let's fetch rows from the table
	err = getRowsFromTable(db)
	if err != nil {
		fmt.Println("error fetching rows from the table ", err)
	}

	// Let's update rows in the table
	err = updateRowsInTable(db)
	if err != nil {
		fmt.Println("error updating rows in the table ", err)
	}

	// Let's fetch records by id from the table
	err = getById(db)
	if err != nil {
		fmt.Println("error fetching rows from the table by their ID ", err)
	}

}
