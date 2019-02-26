package sdb_test

import (
	"fmt"
	"testing"

	sdb "github.com/streamsdb/driver/go/sdb"
	"github.com/stretchr/testify/assert"
)

func TestAppendAndReadRoundtrip(t *testing.T) {
	conn := sdb.MustOpenDefault()
	defer conn.Close()

	db := conn.DB("sdb-test")

	sid := "stream-id"
	messages := []sdb.MessageInput{
		{Type: "testmessage", Value: []byte("value-1")},
		{Type: "testmessage", Value: []byte("value-2")},
		{Type: "testmessage", Value: []byte("value-3")},
	}

	// stream creation
	pos, err := db.Append(sid, sdb.AnyVersion, messages...)
	assert.NoError(t, err)

	slice, err := db.Read(sid, pos, 10)
	assert.NoError(t, err)

	assert.Equal(t, sid, slice.Stream)
	assert.Equal(t, pos, slice.From)
	assert.Equal(t, pos+2, slice.To)
	assert.Equal(t, pos+3, slice.Next)
	assert.Equal(t, false, slice.HasNext)
	assert.Equal(t, pos+2, slice.Head)
	assert.Equal(t, 3, len(slice.Messages))
}

func TestReadStream(t *testing.T) {
	conn := sdb.MustOpenDefault()
	defer conn.Close()

	db := conn.DB("sdb-test")

	stream := t.Name() + "-stream"
	messages := make([]sdb.MessageInput, 10, 10)
	for i := range messages {
		messages[i] = sdb.MessageInput{
			Value: []byte(fmt.Sprintf("value-%v", i)),
		}
	}

	_, err := db.Append(stream, sdb.AnyVersion, messages...)
	assert.NoError(t, err)

	t.Run("read from end", func(t *testing.T) {
		slice, err := db.Read(stream, -3, 3)
		assert.NoError(t, err)

		assert.Equal(t, sdb.Slice{
			Stream:  stream,
			From:    7,
			To:      10,
			Next:    11,
			HasNext: false,
			Head:    10,
			Messages: []sdb.Message{
				{Header: messages[7].Headers, Value: messages[7].Value},
				{Header: messages[8].Headers, Value: messages[8].Value},
				{Header: messages[9].Headers, Value: messages[9].Value},
			},
		}, slice)
	})
}

/*
func TestWatchStreamCreation(t *testing.T) {
	conn := sdb.MustOpenDefault()
	defer conn.Close()

	col, err := conn.Collection("sdb-test")
	assert.NoError(t, err)
	watch := col.Watch("non-existing-stream", 1, 10)
	select {
	case <-time.After(1 * time.Second):
		break
	case <-watch.Slices:
		t.Error("received slice, stream seems to exist")
		return
	}

	_, err = col.Append("non-existing-stream", []sdb.MessageInput{{Value: []byte("test")}})
	assert.NoError(t, err)

	select {
	case <-time.After(1 * time.Second):
		t.Error("received no slice")
		break
	case <-watch.Slices:
		return
	}
}
*/
