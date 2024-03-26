package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// Entry represents a generic entry
type Entry struct {
	Key       []byte
	Value     []byte
	Timestamp time.Time
}

// MessageStore manages topics and stores topic entries in text files
type MessageStore struct {
	mu     sync.Mutex
	topics map[string]string // Map of topic names to file paths
}

// NewMessageStore creates a new instance of MessageStore
func NewMessageStore() *MessageStore {
	return &MessageStore{
		topics: make(map[string]string),
	}
}

// Delete will remove the specified topic
func (ms *MessageStore) Delete(topic string) error {

	fmt.Println("in message_store delete()")

	filename, ok := ms.topics[topic]
	if !ok {
		//return fmt.Errorf("topic '%s' not found", topic)
		//return nil
		filename = topic + ".data"
	} else {
		delete(ms.topics, topic)
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	// Check if the file exists
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		//fmt.Println("File does not exist")
	} else {
		//fmt.Println("File exists")
		// Delete the topic file
		if err := os.Remove(filename); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to delete topic file: %s, %v", filename, err)
		}
		fmt.Println("File deleted")
	}

	// Delete the index file
	indexFilename := filename + ".idx"
	idx := newIndex(indexFilename)
	err := idx.delete()
	if err != nil {
		return fmt.Errorf("failed to delete topic index file: %v", err)
	}

	return nil
}

// SaveEntry saves an entry to the specified topic
func (ms *MessageStore) SaveEntry(topic string, entry Entry) (int64, error) {

	filename, ok := ms.topics[topic]
	if !ok {
		//return 0, fmt.Errorf("topic '%s' not found", topic)
		ms.topics[topic] = topic + ".data"
	}
	filename, ok = ms.topics[topic]
	if !ok {
		return 0, fmt.Errorf("topic '%s' not found", topic)
	}

	// populate Timestamp if it is empty
	if entry.Timestamp == (time.Time{}) {
		entry.Timestamp = time.Now()
	}

	// Convert Entry to JSON
	entryJSON, err := json.Marshal(entry)
	if err != nil {
		return 0, fmt.Errorf("Error marshaling Entry: %v", err)
	}

	// Convert JSON to byte slice
	entryBytes := []byte(entryJSON)

	offset, err := ms.writeEntry(filename, entryBytes)
	if err != nil {
		return 0, fmt.Errorf("Error writing entry: %v", err)
	}

	return offset, nil
}

// writeEntry writes an entry to the specified topic file
func (ms *MessageStore) writeEntry(filename string, entry []byte) (int64, error) {

	ms.mu.Lock()
	defer ms.mu.Unlock()

	file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return 0, fmt.Errorf("Error opening file: %v", err)
	}
	defer file.Close()

	/*
		if numberOfBytesWritten, err := file.Write(entry); err != nil {
			return err
		}
		if numberOfBytesWritten, err := file.WriteString("\n"); err != nil {
			return err
		}
	*/

	// Write the entry to the file
	numberOfBytesWritten, err := fmt.Fprintf(file, "%s\n", entry)
	if err != nil {
		return 0, fmt.Errorf("failed to write entry to index file: %v", err)
	}

	// Get the current offset in the file
	fileInfo, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to get file info: %v", err)
	}
	position := fileInfo.Size() - int64(numberOfBytesWritten)

	indexFilename := filename + ".idx"
	idx := newIndex(indexFilename)
	count, offset, err := idx.getMaxOffset()
	if err != nil {
		return 0, fmt.Errorf("failed to get getMaxOffset: %v", err)
	}
	if count == 0 {
		offset = 0
	} else {
		offset = offset + 1
	}

	// write the index entry corresponding to the offset
	indexEntry := &IndexEntry{
		Offset: offset,
		Pos:    position,
		Length: int64(numberOfBytesWritten),
	}
	err = idx.writeIndexEntry(*indexEntry)
	if err != nil {
		return 0, fmt.Errorf("error writing index entry to file: %s, %v", indexFilename, err)
	}

	return offset, nil
}

// ReadEntry reads an entry from the given offset from the specified topic
func (ms *MessageStore) ReadEntry(topic string, offset int64) (*Entry, error) {

	filename, ok := ms.topics[topic]
	if !ok {
		//return nil, fmt.Errorf("topic '%s' not found", topic)
		ms.topics[topic] = topic + ".data"
	}
	filename, ok = ms.topics[topic]
	if !ok {
		return nil, fmt.Errorf("topic '%s' not found", topic)
	}

	entryBytes, err := ms.readEntry(filename, offset)
	if err != nil {
		return nil, fmt.Errorf("error reading entry from file: %v", err)
	}

	// Convert JSON slice of bytes to Entry
	var entry Entry
	if err := json.Unmarshal(entryBytes, &entry); err != nil {
		return nil, fmt.Errorf("error unmarshaling Entry: %v", err)
	}

	return &entry, nil
}

// readEntry reads an entry from the specified topic file
func (ms *MessageStore) readEntry(filename string, offset int64) ([]byte, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	// Open the topic file for reading
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open topic file: %v", err)
	}
	defer file.Close()

	// Get the index entry corresponding to the offset
	indexFilename := filename + ".idx"
	idx := newIndex(indexFilename)
	indexEntry, err := idx.getIndexEntry(offset)
	if err != nil {
		return nil, fmt.Errorf("error getting index entry from file: %s, %v", indexFilename, err)
	}

	// Seek to the position of the entry in the topic file
	if _, err := file.Seek(indexEntry.Pos, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to position %d in topic file: %v", indexEntry.Pos, err)
	}

	// Read the bytes corresponding to the length of the entry
	entryBytes := make([]byte, indexEntry.Length)
	if _, err := file.Read(entryBytes); err != nil {
		return nil, fmt.Errorf("failed to read entry from topic file: %v", err)
	}

	return entryBytes, nil
}
