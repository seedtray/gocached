package main

import (
	"fmt"
	"time"
)

const (
	GCDelay          = 60
	GenerationSize   = 60
	StorageThreshold = 5000
)

var timer = func(updatesChannel chan UpdateMessage, frequency int64) {
	for {
		time.Sleep(time.Duration(1e9 * frequency)) // one second * GCDelay
		updatesChannel <- UpdateMessage{Collect, "", time.Now().Unix(), 0}
	}
}

func (self *UpdateMessage) getCurrentTimeSlot() int64 {
	return roundTime(self.currentEpoch)
}

func (self *UpdateMessage) getNewTimeSlot() int64 {
	return roundTime(self.newEpoch)
}

func roundTime(time int64) int64 {
	return time - (time % GenerationSize) + GenerationSize
}

type Generation struct {
	startEpoch  int64
	inhabitants map[string]bool
}

func (self *Generation) String() string {
	r := fmt.Sprintf("Generation [%s-%s]", time.Unix(self.startEpoch, 0).UTC(), time.Unix(self.startEpoch+GenerationSize, 0).UTC())
	for key, _ := range self.inhabitants {
		r += fmt.Sprintf("\n %s", key)
	}
	return r
}

func newGeneration(epoch int64) *Generation {
	return &Generation{epoch, make(map[string]bool)}
}

type GenerationalStorage struct {
	generations    map[int64]*Generation
	updatesChannel chan UpdateMessage
	cacheStorage   CacheStorage
	lastCollected  int64
	items          uint64
}

func newGenerationalStorage(expiring_frequency int64, cacheStorage CacheStorage, updatesChannel chan UpdateMessage) *GenerationalStorage {
	storage := &GenerationalStorage{make(map[int64]*Generation), updatesChannel, cacheStorage, roundTime(time.Now().Unix()) - GenerationSize, 0}
	go timer(updatesChannel, expiring_frequency)
	go processNodeChanges(storage, updatesChannel)
	return storage
}

func (self *GenerationalStorage) removeGenerationToCollect(now int64) *Generation {
	if now >= self.lastCollected+GenerationSize {
		gen := self.generations[now]
		delete(self.generations, now)
		self.lastCollected += GenerationSize
		return gen
	}
	return nil
}

func (self *GenerationalStorage) findGeneration(timeSlot int64, createIfNotExists bool) *Generation {
	generation := self.generations[timeSlot]
	if generation == nil && createIfNotExists {
		generation = newGeneration(timeSlot)
		self.generations[timeSlot] = generation
	}
	return generation
}

func (self *Generation) addInhabitant(key string) {
	self.inhabitants[key] = true
}

func processNodeChanges(storage *GenerationalStorage, channel <-chan UpdateMessage /*, ticker *time.Ticker*/) {
	for {
		msg := <-channel
		switch msg.op {
		case Add:
			timeSlot := msg.getNewTimeSlot()
			generation := storage.findGeneration(timeSlot, true)
			generation.inhabitants[msg.key] = true
			storage.items += 1
		case Delete:
			timeSlot := msg.getCurrentTimeSlot()
			if generation := storage.findGeneration(timeSlot, false); generation != nil {
				delete(generation.inhabitants, msg.key)
				storage.items -= 1
			}
		case Change:
			timeSlot := msg.getCurrentTimeSlot()
			if generation := storage.findGeneration(timeSlot, false); generation != nil {
				delete(generation.inhabitants, msg.key)
			}
			newTimeSlot := msg.getNewTimeSlot()
			generation := storage.findGeneration(newTimeSlot, true)
			generation.addInhabitant(msg.key)
		case Collect:
			for {
				generation := storage.removeGenerationToCollect(msg.getCurrentTimeSlot() - GenerationSize)
				if generation == nil {
					break
				}
				for key, _ := range generation.inhabitants {
					storage.cacheStorage.Expire(key, false)
					storage.items -= 1
				}
			}
			if storage.items > StorageThreshold {
				permGen := storage.findGeneration(GenerationSize, true)
				delete(storage.generations, GenerationSize)
				for key, _ := range permGen.inhabitants {
					storage.cacheStorage.Expire(key, false)
					storage.items -= 1
				}
			}
		}
	}
}
