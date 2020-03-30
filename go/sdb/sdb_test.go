package sdb_test

import (
	"fmt"
	"log"

	sdb "github.com/streamsdb/driver/go/sdb"
)

func ExampleOpenDefault() {
	client, err := sdb.OpenDefault()
	if err != nil {
		log.Fatal("connect error", err)
	}

	if err := client.Ping(); err != nil {
		log.Fatal("ping error", err)
	}

	fmt.Println("Connected to StreamsDB!")

	// Output: Connected to StreamsDB!
}

func ExampleDB_AppendStream() {
	client, err := sdb.OpenDefault()
	if err != nil {
		log.Fatal("connect error", err)
	}

	db := client.DB("")

	// append message to stream
	_, err = db.AppendStream("example", sdb.AnyVersion, sdb.MessageInput{
		Type:  "string",
		Value: []byte("hello"),
	})

	if err != nil {
		log.Fatal("append error", err)
	}

	fmt.Println("written to stream")

	// Output: written to stream
}

func ExampleDB_OpenStreamForward() {
	client, err := sdb.OpenDefault()
	if err != nil {
		log.Fatal("connect error", err)
	}

	db := client.DB("")

	// append 3 messages to stream
	_, err = db.AppendStream("example", sdb.AnyVersion, sdb.MessageInput{
		Type:  "string",
		Value: []byte("hello"),
	},
		sdb.MessageInput{
			Type:  "string",
			Value: []byte("world"),
		},
		sdb.MessageInput{
			Type:  "string",
			Value: []byte("!"),
		})

	if err != nil {
		log.Fatal("write error", err)
	}

	// read the messages from the stream
	iterator, err := db.OpenStreamForward("example", sdb.StreamReadOptions{
		From: 1,
	})
	if err != nil {
		log.Fatal("open stream error", err)
	}

	defer iterator.Close()

	for iterator.Advance() {
		message, err := iterator.Get()
		if err != nil {
			log.Fatal("read error", err)
		}

		fmt.Println(string(message.Value))
	}

	// Output:
	// hello
	// world
	// !
}

func ExampleDB_ReadStreamForward() {
	client, err := sdb.OpenDefault()
	if err != nil {
		log.Fatal("connect error", err)
	}

	db := client.DB("")

	// append 3 messages to stream
	position, err := db.AppendStream("example", sdb.AnyVersion, sdb.MessageInput{
		Type:  "string",
		Value: []byte("hello"),
	},
		sdb.MessageInput{
			Type:  "string",
			Value: []byte("world"),
		},
		sdb.MessageInput{
			Type:  "string",
			Value: []byte("!"),
		})

	if err != nil {
		log.Fatal("write error", err)
	}

	// read the messages from the stream
	slice, err := db.ReadStreamForward("example", position, 10)
	if err != nil {
		log.Fatal("read error", err)
	}

	for _, message := range slice.Messages {
		fmt.Println(string(message.Value))
	}

	// Output:
	// hello
	// world
	// !
}

func ExampleDB_ReadStreamBackward() {
	client, err := sdb.OpenDefault()
	if err != nil {
		log.Fatal("connect error", err)
	}

	db := client.DB("")

	// append 3 messages to stream
	_, err = db.AppendStream("example", sdb.AnyVersion,
		sdb.MessageInput{Value: []byte("hello")},
		sdb.MessageInput{Value: []byte("world")},
		sdb.MessageInput{Value: []byte("!")})

	if err != nil {
		log.Fatal("write error", err)
	}

	// read the messages from the stream
	slice, err := db.ReadStreamBackward("example", -1, 3)
	if err != nil {
		log.Fatal("read error", err)
	}

	for _, message := range slice.Messages {
		fmt.Println(string(message.Value))
	}

	// Output:
	// !
	// world
	// hello
}
