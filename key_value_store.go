package simpledb

import (
	"bytes"
)

type KeyValueStore struct {
	//to start with,we will convert all data type to []byte
	// and store that as string type key or bytes value
	mem  map[string][]byte
	disk WAL
}

func (kv *KeyValueStore) Open() error {
	var err error
	kv.mem = map[string][]byte{}
	if err = kv.disk.Open(); err != nil {
		return err
	}

	//on restart,populate database from disk
	done := false
	for {
		ent := WalEntry{}
		done, err = kv.disk.Read(&ent)
		if err != nil {
			return err
		}
		if done {
			break
		}

		if ent.deleted {
			delete(kv.mem, string(ent.key))
		} else {
			kv.mem[string(ent.key)] = ent.val
		}
	}

	return err
}

func (kv *KeyValueStore) Close() error {
	kv.mem = nil
	return kv.disk.Close()
}

func (kv *KeyValueStore) Set(key []byte, value []byte) (updated bool, err error) {
	v, ok := kv.mem[string(key)]
	if ok && bytes.Equal(v, value) {
		return false, nil
	}

	//write to memory
	kv.mem[string(key)] = value

	//write to disk
	err = kv.disk.Write(&WalEntry{
		key: key,
		val: value,
	})

	return true, err
}

func (kv *KeyValueStore) Get(key []byte) (value []byte, ok bool, err error) {
	v, ok := kv.mem[string(key)]
	return v, ok, nil
}

func (kv *KeyValueStore) Del(key []byte) (updated bool, err error) {
	_, ok := kv.mem[string(key)]
	if !ok {
		return false, nil
	}

	//delete from memory
	delete(kv.mem, string(key))
	//write a TOMBSTONE to disk
	err = kv.disk.Write(&WalEntry{
		key:     key,
		deleted: true,
	})

	return true, nil
}
