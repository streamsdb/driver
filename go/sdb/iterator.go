package sdb

type SliceIterator interface {
	Advance() bool
	Get() (Slice, error)
}

type MessageIterator interface {
	Advance() bool
	Get() (Message, error)
}

type messageIterator struct {
	done     bool
	message  Message
	err      error
	messages []Message

	slices SliceIterator
}

func (iterator *messageIterator) Advance() bool {
	if iterator.done {
		return false
	}

	if len(iterator.messages) > 0 {
		iterator.message = iterator.messages[0]
		iterator.messages = iterator.messages[1:]
		return true
	}

	if !iterator.slices.Advance() {
		iterator.done = true
		return false
	}

	slice, err := iterator.slices.Get()
	if err != nil {
		iterator.err = err
		return true
	}

	if len(slice.Messages) > 0 {
		iterator.message = slice.Messages[0]
		iterator.messages = slice.Messages[1:]
		return true
	}

	iterator.done = true
	return false
}

func (iterator *messageIterator) Get() (Message, error) {
	return iterator.message, iterator.err
}

func (iterator *sliceIterator) Messages() MessageIterator {
	return &messageIterator{
		slices: iterator,
	}
}

type sliceIterator struct {
	done    bool
	slice   Slice
	err     error
	from    int64
	reverse bool

	read func(from int64, reverse bool) (Slice, error)
}

func (iterator *sliceIterator) Advance() bool {
	if iterator.done {
		return false
	}

	iterator.slice, iterator.err = iterator.read(iterator.from, iterator.reverse)
	if iterator.err != nil {
		return true
	}

	iterator.from = iterator.slice.Next
	iterator.done = !iterator.slice.HasNext
	return true
}

func (iterator *sliceIterator) Get() (Slice, error) {
	return iterator.slice, iterator.err
}
