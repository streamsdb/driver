package client_test

import (
	"testing"
	"time"

	"github.com/pjvds/streamsdb/client"
	"github.com/stretchr/testify/assert"
)

func TestWatchStreamCreation(t *testing.T) {
	conn := client.MustOpenDefault()
	defer conn.Close()

	col, err := conn.Collection("client-test")
	assert.NoError(t, err)
	watch := col.Watch("non-existing-stream", 1, 10)
	select {
	case <-time.After(1 * time.Second):
		break
	case <-watch.Slices:
		t.Error("received slice, stream seems to exist")
		return
	}

	_, err = col.Append("non-existing-stream", []client.MessageInput{{Value: []byte("test")}})
	assert.NoError(t, err)

	select {
	case <-time.After(1 * time.Second):
		t.Error("received no slice")
		break
	case <-watch.Slices:
		return
	}
}
