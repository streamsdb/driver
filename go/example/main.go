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
	conn := sdb.MustOpen("sdb://sdb-01.streamsdb.io:443/default?tls=1&block=1")
	defer conn.Close()

	// get database reference
	// specify empty string to use the database "example" from the connection string
	db := conn.DB("")

	// create a channel to get notified from any errors
	errs := make(chan error)

	// read user input from stdin and append it to the stream
	go func() {
		println("type a message and press [enter]")

		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			_, err := db.Append("inputs", sdb.AnyVersion, sdb.MessageInput{Value: scanner.Bytes()})
			if err != nil {
				errs <- errors.Wrap(err, "append error")
			}
		}
	}()

	// subscribe to the inputs stream and print messages
	go func() {
		subscription := db.Subscribe("inputs", -1, 10)
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
