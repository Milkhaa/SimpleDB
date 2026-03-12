package simpledb

type KeyValueStore struct {
	//to start with,we will convert all data type to []byte
	// and store that as string type key or value
	mem map[string]string
}

func (kv *KeyValueStore) Open() error {
	kv.mem = make(map[string]string, 0)
	return nil
}

func (kv *KeyValueStore) Close() error {
	kv.mem = nil
	return nil
}

func (kv *KeyValueStore) Set(key []byte, value []byte) (updated bool, err error) {
	v, ok := kv.mem[string(key)]
	if ok && v == string(value) {
		return false, nil
	}

	// convert bytes to string before storing
	kv.mem[string(key)] = string(value)
	return true, nil
}

func (kv *KeyValueStore) Get(key []byte) (value []byte, ok bool, err error) {
	v, ok := kv.mem[string(key)]
	return []byte(v), ok, nil
}

func (kv *KeyValueStore) Del(key []byte) (updated bool, err error) {
	_, ok := kv.mem[string(key)]
	if !ok {
		return false, nil
	}

	delete(kv.mem, string(key))
	return true, nil
}

//JaiShreeRam
