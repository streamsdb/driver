package main

import (
	"bufio"
	"log"
	"os"
	"time"

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
				println(err)
				errs <- errors.Wrap(err, "append error")
			}
		}
	}()

	// subscribe to the inputs stream and print messages
	go func() {
	READ:
		iterator, err := db.OpenStreamForward("inputs", sdb.StreamReadOptions{
			KeepOpen: true,
		})
		if err != nil {
			println("read error: ", err.Error())
			time.Sleep(1 * time.Second)
			goto READ
		}

		for iterator.Advance() {
			msg, err := iterator.Get()
			if err != nil {
				println("read error: ", err.Error())
				iterator.Close()
				time.Sleep(1 * time.Second)
				goto READ
			}

			println("received: ", string(msg.Value))
		}

		println("EOF")
		iterator.Close()
		time.Sleep(1 * time.Second)
		goto READ

		// the subscription has been closed, write error to channel
		errs <- errors.New("done")
	}()

	// blocking till an error is received
	err := <-errs
	log.Fatalf("fatal error: %s", err)
}
