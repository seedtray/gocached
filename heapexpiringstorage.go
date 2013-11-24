package main

import (
	"container/heap"
	"github.com/julian-gutierrez-o/gocached/expiry"
	"time"
)

//Implements a CacheStorage interface with entry expiration.
type HeapExpiringStorage struct {
	CacheStorage
	updatesChannel chan UpdateMessage
	heap           *expiry.Heap
}

func (hs *HeapExpiringStorage) ProcessUpdates() {
	for {
		msg := <-hs.updatesChannel
		switch msg.op {
		case Add, Change:
			hs.AddEntry(expiry.Entry{&msg.key, uint32(msg.newEpoch)}, uint32(msg.currentEpoch))
		case Collect:
			logger.Println("Collecting expired entries")
			hs.Collect(uint32(msg.currentEpoch))
		}
	}
}

//Update. Given an exptime update, stores a new entry in a exptime ordered heap
func (hs *HeapExpiringStorage) AddEntry(entry expiry.Entry, now uint32) {
	if entry.Exptime > now {
		heap.Push(hs.heap, entry)
	}
}

// Inspects the exptime heap for candidates for expiration, and dispatches to storage.Expire. The heap won't contain any expired entry refs(*) when it exits
func (hs *HeapExpiringStorage) Collect(now uint32) {
	h := hs.heap
	if h.Len() == 0 {
		return
	}
	logger.Printf("heap size: %v", h.Len())
	for h.Len() > 0 {
		tip := h.Tip()
		if tip.Exptime > now {
			break
		}
		h.Pop()
		logger.Println("trying to expire %s at %v", *tip.Key, now)
		hs.CacheStorage.Expire(*tip.Key, true)
	}
}

//Allocate a new HeapExpiringStorage and Initialize it
func NewHeapExpiringStorage(collect_frequency int64, cacheStorage CacheStorage, updatesChannel chan UpdateMessage) *HeapExpiringStorage {
	hs := &HeapExpiringStorage{cacheStorage, updatesChannel, nil}
	hs.Init(collect_frequency)
	return hs
}

//Init an allocated HeapExpiringStorage
func (hs *HeapExpiringStorage) Init(collect_frequency int64) {
	logger.Println("init heap notify storage")
	hs.heap = expiry.NewHeap(100) //TODO, size as config parameter
	go hs.CollectTicker(collect_frequency)
	go hs.ProcessUpdates()
}

func (hs *HeapExpiringStorage) CollectTicker(collect_frequency int64) {
	for {
		time.Sleep(time.Duration(1e9 * collect_frequency))
		hs.updatesChannel <- UpdateMessage{Collect, "", time.Now().Unix(), 0}
	}
}
