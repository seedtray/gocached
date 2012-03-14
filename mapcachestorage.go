package main

import (
	"strconv"
	"sync"
	"time"
)

type MapCacheStorage struct {
	storageMap map[string]*StorageEntry
	rwLock     sync.RWMutex
}

func newMapCacheStorage() *MapCacheStorage {
	storage := &MapCacheStorage{}
	storage.Init()
	return storage
}

func (self *MapCacheStorage) Init() {
	self.storageMap = make(map[string]*StorageEntry)
}

func (self *StorageEntry) expired() bool {
	if self.exptime == 0 {
		return false
	}
	now := uint32(time.Now().Unix())
	return self.exptime <= now
}

func (self *MapCacheStorage) Set(key string, flags uint32, exptime uint32, bytes uint32, content []byte) (previous *StorageEntry, result *StorageEntry) {
	self.rwLock.Lock()
	defer self.rwLock.Unlock()
	entry, present := self.storageMap[key]
	var newEntry *StorageEntry
	if present && !entry.expired() {
		newEntry = &StorageEntry{exptime, flags, bytes, entry.cas_unique + 1, content}
		self.storageMap[key] = newEntry
		return entry, newEntry
	}
	newEntry = &StorageEntry{exptime, flags, bytes, 0, content}
	self.storageMap[key] = newEntry
	return nil, newEntry
}

func (self *MapCacheStorage) Add(key string, flags uint32, exptime uint32, bytes uint32, content []byte) (err ErrorCode, result *StorageEntry) {
	self.rwLock.Lock()
	defer self.rwLock.Unlock()
	entry, present := self.storageMap[key]
	if present && !entry.expired() {
		return KeyAlreadyInUse, nil
	}
	entry = &StorageEntry{exptime, flags, bytes, 0, content}
	self.storageMap[key] = entry
	return Ok, entry
}

func (self *MapCacheStorage) Replace(key string, flags uint32, exptime uint32, bytes uint32, content []byte) (ErrorCode, *StorageEntry, *StorageEntry) {
	self.rwLock.Lock()
	defer self.rwLock.Unlock()
	entry, present := self.storageMap[key]
	if present && !entry.expired() {
		newEntry := &StorageEntry{exptime, flags, bytes, entry.cas_unique + 1, content}
		self.storageMap[key] = newEntry
		return Ok, entry, newEntry
	}
	return KeyNotFound, nil, nil
}

func (self *MapCacheStorage) Append(key string, bytes uint32, content []byte) (ErrorCode, *StorageEntry, *StorageEntry) {
	self.rwLock.Lock()
	defer self.rwLock.Unlock()
	entry, present := self.storageMap[key]
	if present && !entry.expired() {
		newContent := make([]byte, len(entry.content)+len(content))
		copy(newContent, entry.content)
		copy(newContent[len(entry.content):], content)
		newEntry := &StorageEntry{entry.exptime, entry.flags, bytes + entry.bytes, entry.cas_unique + 1, newContent}
		self.storageMap[key] = newEntry
		return Ok, entry, newEntry
	}
	return KeyNotFound, nil, nil
}

func (self *MapCacheStorage) Prepend(key string, bytes uint32, content []byte) (ErrorCode, *StorageEntry, *StorageEntry) {
	self.rwLock.Lock()
	defer self.rwLock.Unlock()
	entry, present := self.storageMap[key]
	if present && !entry.expired() {
		newContent := make([]byte, len(entry.content)+len(content))
		copy(newContent, content)
		copy(newContent[len(content):], entry.content)
		newEntry := &StorageEntry{entry.exptime, entry.flags, bytes + entry.bytes,
			entry.cas_unique + 1, newContent}
		self.storageMap[key] = newEntry
		return Ok, entry, newEntry
	}
	return KeyNotFound, nil, nil
}

func (self *MapCacheStorage) Cas(key string, flags uint32, exptime uint32, bytes uint32, cas_unique uint64, content []byte) (ErrorCode, *StorageEntry, *StorageEntry) {
	self.rwLock.Lock()
	defer self.rwLock.Unlock()
	entry, present := self.storageMap[key]
	if present && !entry.expired() {
		if entry.cas_unique == cas_unique {
			newEntry := &StorageEntry{exptime, flags, bytes, cas_unique, content}
			self.storageMap[key] = newEntry
			return Ok, entry, newEntry
		} else {
			return IllegalParameter, entry, nil
		}
	}
	return KeyNotFound, nil, nil
}

func (self *MapCacheStorage) Get(key string) (ErrorCode, *StorageEntry) {
	self.rwLock.RLock()
	defer self.rwLock.RUnlock()
	entry, present := self.storageMap[key]
	if present && !entry.expired() {
		return Ok, entry
	}
	return KeyNotFound, nil
}

func (self *MapCacheStorage) Delete(key string) (ErrorCode, *StorageEntry) {
	self.rwLock.Lock()
	defer self.rwLock.Unlock()
	entry, present := self.storageMap[key]
	if present && !entry.expired() {
		delete(self.storageMap, key)
		return Ok, entry
	}
	return KeyNotFound, nil
}

func (self *MapCacheStorage) Incr(key string, value uint64, incr bool) (ErrorCode, *StorageEntry, *StorageEntry) {
	self.rwLock.Lock()
	defer self.rwLock.Unlock()
	entry, present := self.storageMap[key]
	if present && !entry.expired() {
		if addValue, err := strconv.ParseUint(string(entry.content), 10, 64); err == nil {
			var incrValue uint64
			if incr {
				incrValue = uint64(addValue) + value
			} else if value > addValue {
				incrValue = 0
			} else {
				incrValue = uint64(addValue) - value
			}
			incrStrValue := strconv.FormatUint(incrValue, 10)
			old_value := entry.content
			entry.content = []byte(incrStrValue)
			entry.bytes = uint32(len(entry.content))
			entry.cas_unique += 1
			return Ok, &StorageEntry{entry.exptime, entry.flags, entry.bytes, entry.cas_unique, old_value}, entry
		} else {
			return IllegalParameter, nil, nil
		}
	}
	return KeyNotFound, nil, nil
}

/* keep a null object for map deletion */
var nullStorageEntry = &StorageEntry{}

func (self *MapCacheStorage) Expire(key string, check bool) {
	self.rwLock.Lock()
	defer self.rwLock.Unlock()
	entry, present := self.storageMap[key]
	if present && (!check || entry.expired()) {
		delete(self.storageMap, key)
	}
}
