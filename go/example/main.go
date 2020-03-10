package main

import (
	"bufio"
	"log"
	"os"

	"github.com/pkg/errors"
	"github.com/streamsdb/driver/go/sdb"
)

func main() {
	// create streamsdb connection
	conn := sdb.MustOpenDefault()
	defer conn.Close()

	// get database reference
	// specify empty string to use the database "example" from the connection string
	db := conn.DB("")

	if err := db.EnsureExists(); err != nil {
		println("ensure database exists failed: ", err.Error())
		return
	}

	if err := conn.Ping(); err != nil {
		println("ping failed to StreamsDB: ", err.Error())
		return
	}
	println("ping to success")

	// create a channel to get notified from any errors
	errs := make(chan error)

	// read user input from stdin and append it to the stream
	go func() {
		println("type a message and press [enter]")

		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			println(">> appending to stream")
			_, err := db.AppendStream("inputs", sdb.AnyVersion, sdb.MessageInput{Type: "string", Value: scanner.Bytes()})
			if err != nil {
				errs <- errors.Wrap(err, "append error")
			}
		}
	}()

	// subscribe to the inputs stream and print messages
	go func() {
		subscription := db.SubscribeStream("inputs", -1, 10)
		for slice := range subscription.Slices {
			for _, msg := range slice.Messages {
				println("received: ", string(msg.Value))
			}
		}

		// the subscription has been closed, write error to channel
		errs <- errors.Wrap(subscription.Err(), "read error")
	}()

	// blocking till an error is received
	log.Fatalf((<-errs).Error())
}
