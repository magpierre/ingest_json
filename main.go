package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"time"
	"unicode/utf8"

	client "github.com/mapr/maprdb-go-client"
)

func buildConnectionString(maprURL *string, auth *string, user *string, password *string, ssl *bool) string {
	var connectionString = *maprURL
	if auth != nil {
		connectionString += fmt.Sprintf("?%s=%s", "auth", *auth)
	}
	if user != nil {
		connectionString += fmt.Sprintf(";%s=%s", "user", *user)
	}
	if password != nil && len(*password) > 0 {
		connectionString += fmt.Sprintf(";%s=%s", "password", *password)
	} else {
		fmt.Println("Password for connection to MapR needs to be set")
		return ""
	}
	if ssl != nil {
		connectionString += fmt.Sprintf(";%s=%t", "ssl", *ssl)
	}
	return connectionString
}

func connectMapR(connectionString string, storeName string) (*client.Connection, *client.DocumentStore) {

	var store *client.DocumentStore
	connection, err := client.MakeConnection(connectionString)
	if err != nil {
		panic(err)
	}
	if exists, _ := connection.IsStoreExists(storeName); exists == false {
		fmt.Printf("Creating store: %s\n", storeName)
		store, err = connection.CreateStore(storeName)
	} else {
		fmt.Printf("Get store: %s\n", storeName)
		store, err = connection.GetStore(storeName)
	}
	if err != nil {
		panic(err)
	}
	return connection, store
}

func main() {
	fname := flag.String("filename", "", "File to load")
	separator := flag.String("separator", ",", "separator")
	comment := flag.String("comment", "#", "Comment")
	quotes := flag.Bool("lazyQuotes", true, "Lazy Quotes")
	maprURL := flag.String("mapr-url", "localhost:5678", "The URL to mapr in the form localhost:5678")
	auth := flag.String("auth", "basic", "Authorization type")
	user := flag.String("user", "mapr", "Username for the connection")
	password := flag.String("password", "", "Password for the user")
	ssl := flag.Bool("use-ssl", false, "Use SSL? (true)")
	storeName := flag.String("mapr-tablename", "/demo/tables/data_ingest", "Table to store the MQTT messages")

	flag.Parse()

	connectionString := buildConnectionString(maprURL, auth, user, password, ssl)

	fmt.Println("Formatted connection string to MapR:")
	fmt.Println(connectionString)

	/* Application starts here */
	connection, store := connectMapR(connectionString, *storeName)

	f, err := os.Open(*fname)
	if err != nil {
		println("File error:", err)
		return
	}

	r := csv.NewReader(f)
	val, _ := utf8.DecodeRuneInString(*separator)
	comm, _ := utf8.DecodeRuneInString(*comment)

	r.Comma = val
	r.Comment = comm
	r.LazyQuotes = *quotes

	firstLine := 1
	var header []string
	var counter int

	doc, _ := connection.CreateEmptyDocument()

	for {
		indata, err := r.Read()
		if err != nil && indata == nil {
			fmt.Println(err)
			break
		}
		if firstLine == 1 {
			header = indata
			firstLine = 0
			continue
		}
		counter++
		for i := 0; i < len(indata); i++ {
			if i == 0 {
				doc.SetIdString(indata[0] + "_" + fmt.Sprintf("%s", time.Now()))
				continue
			}
			doc.SetString(header[i], indata[i])
		}

		// Now we store the document in the DB
		err = store.InsertDocument(doc)
		if err != nil {
			fmt.Println(err)
			continue
		}

		doc.Clean()

	}

	fmt.Printf("Ingested %d documents into DB\n", counter)
	connection.Close()

}
