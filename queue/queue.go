package queue

import (
	"encoding/binary"
	"errors"
	"path/filepath"
	"runtime"
	"time"

	"github.com/dgraph-io/badger/v3"
)

type Queue struct {
	db *badger.DB
}

type Item struct {
	key   []byte
	Value []byte
}

func (i Item) Key() []byte {
	key := make([]byte, len(i.key))
	copy(key, i.key)
	return key
}

func New(name string) (*Queue, error) {
	n := runtime.NumCPU()
	if n >= 8 {
		n = n / 2
	}
	db, err := badger.Open(badger.LSMOnlyOptions(filepath.Join(".", name)).WithNumGoroutines(n).WithSyncWrites(false))
	return &Queue{db}, err
}

func (q *Queue) Close() error {
	return q.db.Close()
}

func (q *Queue) Push(value []byte) error {
	key := [8]byte{}
	now := time.Now().UnixNano()
	binary.BigEndian.PutUint64(key[:], uint64(now))
	return q.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key[:], value)
	})
}

func (q *Queue) Peek() (Item, error) {
	key := []byte(nil)
	value := []byte(nil)
	err := q.db.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		iter.Rewind()
		first := iter.Item()
		iter.Close()
		if first == nil {
			return errors.New("queue is empty")
		}
		val, err := first.ValueCopy(nil)
		if err != nil {
			return err
		}
		key = first.Key()
		value = val
		return nil
	})
	return Item{key: key, Value: value}, err
}

func (q *Queue) Pop(key []byte) error {
	return q.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

func (q *Queue) Next(prev []byte) (Item, error) {
	key := []byte(nil)
	value := []byte(nil)
	err := q.db.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		iter.Seek(prev)
		iter.Next()
		next := iter.Item()
		iter.Close()
		if next == nil {
			return errors.New("queue is empty")
		}
		val, err := next.ValueCopy(nil)
		if err != nil {
			return err
		}
		key = next.Key()
		value = val
		return nil
	})
	return Item{key: key, Value: value}, err
}
