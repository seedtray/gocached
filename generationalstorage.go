package main

import (
  "time"
  "fmt"
)

const (
  GCDelay = 60
  GenerationSize = 60
  StorageThreshold = 5000
)

var timer = func(updatesChannel chan UpdateMessage, frequency int64) {
  for {
    time.Sleep(1e9 * frequency) // one second * GCDelay
    updatesChannel <- UpdateMessage{Collect, "", time.Seconds(), 0}
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
  startEpoch    int64
  inhabitants   map[string] bool
}

func (self *Generation) String() string {
  r := fmt.Sprintf("Generation [%s-%s]", time.SecondsToUTC(self.startEpoch), time.SecondsToUTC(self.startEpoch + GenerationSize))
  for key,_ := range(self.inhabitants) {
    r += fmt.Sprintf("\n %s", key)
  }
  return r
}

func newGeneration(epoch int64) *Generation {
  return &Generation{epoch, make(map[string] bool)}
}

type GenerationalStorage struct {
  generations     map[int64] *Generation
  updatesChannel  chan UpdateMessage
  cacheStorage    CacheStorage
  lastCollected   int64
  items           uint64
}

func newGenerationalStorage(expiring_frequency int64, cacheStorage CacheStorage, updatesChannel chan UpdateMessage) *GenerationalStorage {
  storage := &GenerationalStorage{ make(map [int64] *Generation), updatesChannel, cacheStorage, roundTime(time.Seconds()) - GenerationSize, 0 }
  go timer(updatesChannel, expiring_frequency)
  go processNodeChanges(storage, updatesChannel)
  return storage;
}

func (self *GenerationalStorage) removeGenerationToCollect(now int64) *Generation {
  if now >= self.lastCollected + GenerationSize {
    gen := self.generations[now]
    self.generations[now] = nil, false
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
        generation.inhabitants[msg.key] = false, false
        storage.items -= 1
      }
    case Change:
      timeSlot := msg.getCurrentTimeSlot()
      if generation := storage.findGeneration(timeSlot, false); generation != nil {
        generation.inhabitants[msg.key] = false, false
      }
      newTimeSlot := msg.getNewTimeSlot()
      generation := storage.findGeneration(newTimeSlot, true)
      generation.addInhabitant(msg.key)
    case Collect:
      logger.Println("Processing Collect message")
      for {
        generation := storage.removeGenerationToCollect(msg.getCurrentTimeSlot()- GenerationSize)
        if generation == nil {
          break
        }
        for key , _ := range(generation.inhabitants) {
          storage.cacheStorage.Expire(key, false)
          storage.items -= 1
        }
      }
      if storage.items > StorageThreshold {
        permGen := storage.findGeneration(GenerationSize, true)
        storage.generations[GenerationSize] = nil, false
        for key , _ := range(permGen.inhabitants) {
          storage.cacheStorage.Expire(key, false)
          storage.items -= 1
        }
        logger.Printf("Memory pressure. Collecting %d expiring items. %d items on permanent generation", storage.items)
      }
      logger.Printf("No more items to collect. %d Items", storage.items)
    }
  }
}
