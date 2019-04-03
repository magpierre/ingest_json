/*

   Copyright 2019 MapR Technologies

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

*/

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"time"

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
	maprURL := flag.String("mapr-url", "localhost:5678", "The URL to mapr in the form localhost:5678")

	auth := flag.String("auth", "basic", "Authorization type")
	user := flag.String("user", "mapr", "Username for the connection")
	password := flag.String("password", "", "Password for the user")
	ssl := flag.Bool("use-ssl", false, "Use SSL? (true)")
	storeName := flag.String("mapr-tablename", "", "Table to store the json as a document")

	flag.Parse()

	var data []byte
	var err error

	switch flag.NArg() {
	case 0:
		data, err = ioutil.ReadAll(os.Stdin)
		break
	case 1:
		data, err = ioutil.ReadFile(flag.Arg(0))
		if err != nil {
			fmt.Println(err)
			os.Exit(-1)
		}
		break
	default:
		fmt.Printf("input must be from stdin or file\n")
		os.Exit(1)
	}

	connectionString := buildConnectionString(maprURL, auth, user, password, ssl)
	fmt.Println("Formatted connection string to MapR:")
	fmt.Println(connectionString)

	/* Application starts here */
	connection, store := connectMapR(connectionString, *storeName)
	fmt.Println("Connected to MapR Database")
	var f interface{}
	err = json.Unmarshal(data, &f)
	if err != nil {
		fmt.Println(err)
		connection.Close()
		os.Exit(-1)
	}

	switch ft := f.(type) {
	case []interface{}:
		arrayOfDocs := ft
		var cnt int
		for i, val := range arrayOfDocs {
			doc := connection.CreateDocumentFromMap(val.(map[string]interface{}))
			doc.SetIdString(fmt.Sprintf("%s", time.Now()))
			// Now we store the document in the DB
			err = store.InsertDocument(doc)
			if err != nil {
				fmt.Println(err)
				connection.Close()
				os.Exit(-1)
			}
			cnt = i

			if (cnt+1)%1000 == 0 {
				fmt.Printf("%d documents inserted...\n", cnt+1)
			}
		}
		fmt.Println(cnt, " documents inserted.")

	case interface{}:
		doc := connection.CreateDocumentFromMap(ft.(map[string]interface{}))
		err = store.InsertDocument(doc)
		if err != nil {
			fmt.Println(err)
			connection.Close()
			os.Exit(-1)
		}
	}
	fmt.Println("Closing connection...")
	connection.Close()
}
