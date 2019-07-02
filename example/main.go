package main

import (
	"bufio"
	"log"
	"os"

	"github.com/pkg/errors"
	sdb "github.com/streamsdb/driver/go/sdb"
)

func main() {
	// create streamsdb connection
	conn := sdb.MustOpen("sdb://sdb-01.streamsdb.io:443/default?tls=1&gzip=1")
	db := conn.DB("example")
	defer conn.Close()

	// create a channel to get notified from any errors
	errs := make(chan error)

	// read user input from stdin and append it to the stream
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			_, err := db.Append("inputs", -1, sdb.MessageInput{Value: scanner.Bytes()})
			if err != nil {
				errs <- errors.Wrap(err, "append error")
				return
			}
		}
	}()

	// watch the inputs streams for messages and print them
	go func() {
		watch := db.Subscribe("inputs", -1, 10)
		for slice := range watch.Slices {
			for _, msg := range slice.Messages {
				println("received: ", string(msg.Value))
			}
		}

		errs <- errors.Wrap(watch.Err(), "watch error")
	}()

	log.Fatalf((<-errs).Error())
}
