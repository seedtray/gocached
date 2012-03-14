package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

type Session struct {
	conn      *net.TCPConn
	bufreader *bufio.Reader
	storage   CacheStorage
}

type Command interface {
	parse(line []string) bool
	Exec()
}

type StorageCommand struct {
	session    *Session
	command    string
	key        string
	flags      uint32
	exptime    uint32
	bytes      uint32
	cas_unique uint64
	noreply    bool
	data       []byte
}

type RetrievalCommand struct {
	session *Session
	command string
	keys    []string
}

type DeleteCommand struct {
	session *Session
	command string
	key     string
	noreply bool
}

type TouchCommand struct {
	session *Session
	command string
	key     string
	exptime uint32
	noreply bool
}

type IncrCommand struct {
	session *Session
	incr    bool
	key     string
	value   uint64
	noreply bool
}

type UnknownCommand struct {
	session *Session
	command string
}

type UninmplementedCommand struct {
	session *Session
	command string
}

const (
	NA = iota
	InvalidCommand
	ClientError
	ServerError
)

func NewSession(conn *net.TCPConn, store CacheStorage) (*Session, error) {
	var s = &Session{conn, bufio.NewReader(conn), store}
	return s, nil
}

/* Read a line and tokenize it */
func getTokenizedLine(r *bufio.Reader) []string {
	if rawline, _, err := r.ReadLine(); err == nil {
		return strings.Fields(string(rawline))
	}
	return nil
}

func (s *Session) CommandLoop() {

	for line := getTokenizedLine(s.bufreader); line != nil; line = getTokenizedLine(s.bufreader) {
		var cmd Command = cmdSelect(line[0], s)
		if cmd.parse(line) {
			cmd.Exec()
		}
	}
}

func cmdSelect(name string, s *Session) Command {

	switch name {

	case "set", "add", "replace", "append", "prepend", "cas":
		return &StorageCommand{session: s}
	case "get", "gets":
		return &RetrievalCommand{session: s}
	case "delete":
		return &DeleteCommand{session: s}
	case "touch":
		return &TouchCommand{session: s}
	case "incr", "decr":
		return &IncrCommand{session: s}
	case "stats", "flush_all", "version", "quit":
		return &UninmplementedCommand{session: s, command: name}
	default:
		return &UnknownCommand{session: s, command: name}
	}
	//not reaching here
	return nil
}

////////////////////////////// ERROR COMMANDS //////////////////////////////

/* a function to reply errors to client that always returns false */
func Error(s *Session, errtype int, errdesc string) bool {
	var msg string
	switch errtype {
	case InvalidCommand:
		msg = "ERROR\r\n"
	case ClientError:
		msg = "CLIENT_ERROR " + errdesc + "\r\n"
	case ServerError:
		msg = "SERVER_ERROR " + errdesc + "\r\n"
	}
	s.conn.Write([]byte(msg))
	return false
}

func (self *UnknownCommand) parse(line []string) bool {
	return Error(self.session, InvalidCommand, "")
}

func (self *UnknownCommand) Exec() {
}

func (self *UninmplementedCommand) parse(line []string) bool {
	return Error(self.session, ServerError, "Not Implemented")
}

func (self *UninmplementedCommand) Exec() {
}

///////////////////////////// TOUCH COMMAND //////////////////////////////

const secondsInMonth = 60 * 60 * 24 * 30

func (self *TouchCommand) parse(line []string) bool {
	var exptime uint64
	var err error
	if len(line) < 3 {
		return Error(self.session, ClientError, "Bad touch command: missing parameters")
	} else if exptime, err = strconv.ParseUint(line[2], 10, 64); err != nil {
		return Error(self.session, ClientError, "Bad touch command: bad expiration time")
	}
	self.command = line[0]
	self.key = line[1]
	if exptime == 0 || exptime > secondsInMonth {
		self.exptime = uint32(exptime)
	} else {
		self.exptime = uint32(time.Now().Unix()) + uint32(exptime)
	}
	if line[len(line)-1] == "noreply" {
		self.noreply = true
	}
	return true
}

func (self *TouchCommand) Exec() {
	logger.Printf("Touch: command: %s, key: %s, , exptime %d, noreply: %t",
		self.command, self.key, self.exptime, self.noreply)
}

///////////////////////////// DELETE COMMAND ////////////////////////////

func (self *DeleteCommand) parse(line []string) bool {
	if len(line) < 2 {
		return Error(self.session, ClientError, "Bad delete command: missing parameters")
	}
	self.command = line[0]
	self.key = line[1]
	if line[len(line)-1] == "noreply" {
		self.noreply = true
	}
	return true
}

func (self *DeleteCommand) Exec() {
	//  logger.Printf("Delete: command: %s, key: %s, noreply: %t",
	//                self.command, self.key, self.noreply)
	var storage = self.session.storage
	var conn = self.session.conn
	if err, _ := storage.Delete(self.key); err != Ok && !self.noreply {
		conn.Write([]byte("NOT_FOUND\r\n"))
	} else if err == Ok && !self.noreply {
		conn.Write([]byte("DELETED\r\n"))
	}
}

///////////////////////////// RETRIEVAL COMMANDS ////////////////////////////

func (self *RetrievalCommand) parse(line []string) bool {
	if len(line) < 2 {
		return Error(self.session, ClientError, "Bad retrieval command: missing parameters")
	}
	self.command = line[0]
	self.keys = line[1:]
	return true
}

func (self *RetrievalCommand) Exec() {
	//  logger.Printf("Retrieval: command: %s, keys: %s",
	//                self.command, self.keys)
	var storage = self.session.storage
	var conn = self.session.conn
	showAll := self.command == "gets"
	for i := 0; i < len(self.keys); i++ {
		if err, entry := storage.Get(self.keys[i]); err == Ok {
			if showAll {
				conn.Write([]byte(fmt.Sprintf("VALUE %s %d %d %d\r\n", self.keys[i], entry.flags, entry.bytes, entry.cas_unique)))
			} else {
				conn.Write([]byte(fmt.Sprintf("VALUE %s %d %d\r\n", self.keys[i], entry.flags, entry.bytes)))
			}
			conn.Write(entry.content)
			conn.Write([]byte("\r\n"))
		}
	}
	conn.Write([]byte("END\r\n"))
}

///////////////////////////// STORAGE COMMANDS /////////////////////////////

/* parse a storage command parameters and read the related data
   returns a flag indicating sucesss */
func (self *StorageCommand) parse(line []string) bool {
	var flags, exptime, bytes, casuniq uint64
	var err error
	if len(line) < 5 {
		return Error(self.session, ClientError, "Bad storage command: missing parameters")
	} else if flags, err = strconv.ParseUint(line[2], 10, 64); err != nil {
		return Error(self.session, ClientError, "Bad storage command: bad flags")
	} else if exptime, err = strconv.ParseUint(line[3], 10, 64); err != nil {
		return Error(self.session, ClientError, "Bad storage command: bad expiration time")
	} else if bytes, err = strconv.ParseUint(line[4], 10, 64); err != nil {
		return Error(self.session, ClientError, "Bad storage command: bad byte-length")
	} else if line[0] == "cas" {
		if casuniq, err = strconv.ParseUint(line[5], 10, 64); err != nil {
			return Error(self.session, ClientError, "Bad storage command: bad cas value")
		}
	}
	self.command = line[0]
	self.key = line[1]
	self.flags = uint32(flags)
	if exptime == 0 || exptime > secondsInMonth {
		self.exptime = uint32(exptime)
	} else {
		self.exptime = uint32(time.Now().Unix()) + uint32(exptime)
	}
	self.bytes = uint32(bytes)
	self.cas_unique = casuniq
	if line[len(line)-1] == "noreply" {
		self.noreply = true
	}
	return self.readData()
}

/* read the data for a storage command and return a flag indicating success */
func (self *StorageCommand) readData() bool {
	if self.bytes <= 0 {
		return Error(self.session, ClientError, "Bad storage operation: trying to read 0 bytes")
	} else {
		self.data = make([]byte, self.bytes+2) // \r\n is always present at the end
	}
	var reader = self.session.bufreader
	// read all the data
	for offset := 0; offset < int(self.bytes); {
		if nread, err := reader.Read(self.data[offset:]); err != nil {
			return Error(self.session, ServerError, "Failed to read data")
		} else {
			offset += nread
		}
	}
	if string(self.data[len(self.data)-2:]) != "\r\n" {
		return Error(self.session, ClientError, "Bad storage operation: bad data chunk")
	}
	self.data = self.data[:len(self.data)-2] // strip \n\r
	return true
}

func (self *StorageCommand) Exec() {
	/*  logger.Printf("Storage: key: %s, flags: %d, exptime: %d, " +
	    "bytes: %d, cas: %d, noreply: %t, content: %s\n",
	    self.key, self.flags, self.exptime, self.bytes,
	    self.cas_unique, self.noreply, string(self.data))
	*/
	var storage = self.session.storage
	var conn = self.session.conn

	switch self.command {

	case "set":
		storage.Set(self.key, self.flags, self.exptime, self.bytes, self.data)
		if !self.noreply {
			conn.Write([]byte("STORED\r\n"))
		}
		return
	case "add":
		if err, _ := storage.Add(self.key, self.flags, self.exptime, self.bytes, self.data); err != Ok && !self.noreply {
			conn.Write([]byte("NOT_STORED\r\n"))
		} else if err == Ok && !self.noreply {
			conn.Write([]byte("STORED\r\n"))
		}
	case "replace":
		if err, _, _ := storage.Replace(self.key, self.flags, self.exptime, self.bytes, self.data); err != Ok && !self.noreply {
			conn.Write([]byte("NOT_STORED\r\n"))
		} else if err == Ok && !self.noreply {
			conn.Write([]byte("STORED\r\n"))
		}
	case "append":
		if err, _, _ := storage.Append(self.key, self.bytes, self.data); err != Ok && !self.noreply {
			conn.Write([]byte("NOT_STORED\r\n"))
		} else if err == Ok && !self.noreply {
			conn.Write([]byte("STORED\r\n"))
		}
	case "prepend":
		if err, _, _ := storage.Prepend(self.key, self.bytes, self.data); err != Ok && !self.noreply {
			conn.Write([]byte("NOT_STORED\r\n"))
		} else if err == Ok && !self.noreply {
			conn.Write([]byte("STORED\r\n"))
		}
	case "cas":
		if err, prev, _ := storage.Cas(self.key, self.flags, self.exptime, self.bytes, self.cas_unique, self.data); err != Ok && !self.noreply {
			if prev != nil {
				conn.Write([]byte("EXISTS\r\n"))
			} else {
				conn.Write([]byte("NOT_STORED\r\n"))
			}
		} else if err == Ok && !self.noreply {
			conn.Write([]byte("STORED\r\n"))
		}
	}
}

///////////////////////////// INCR/DECR COMMANDS /////////////////////////////

func (self *IncrCommand) parse(line []string) bool {
	var err error
	if len(line) < 3 {
		return Error(self.session, ClientError, "Bad incr/decr command: missing parameters")
	} else if self.value, err = strconv.ParseUint(line[2], 10, 64); err != nil {
		return Error(self.session, ClientError, "Bad incr/decr command: bad value")
	}
	self.incr = (line[0] == "incr")
	self.key = line[1]

	if len(line) == 4 && line[3] == "noreply" {
		self.noreply = true
	} else {
		self.noreply = false
	}
	return true
}

func (self *IncrCommand) Exec() {
	var storage = self.session.storage
	var conn = self.session.conn
	err, _, current := storage.Incr(self.key, self.value, self.incr)
	if self.noreply {
		return
	}
	if err == Ok {
		conn.Write(current.content)
		conn.Write([]byte("\r\n"))
	} else if err == KeyNotFound {
		//not reaching here
		conn.Write([]byte("NOT_FOUND\r\n"))
	} else if err == IllegalParameter {
		conn.Write([]byte(fmt.Sprintf("CLIENT_ERROR cannot increment or decrement non-numeric value\r\n")))
	}
}
