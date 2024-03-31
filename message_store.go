package messagestore

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

	_, ok := ms.topics[topic]
	if !ok {
		//return 0, fmt.Errorf("topic '%s' not found", topic)
		ms.topics[topic] = topic + ".data"
	}
	filename, ok := ms.topics[topic]
	if !ok {
		return 0, fmt.Errorf("failed to find topic '%s'", topic)
	}

	// populate Timestamp if it is empty
	if entry.Timestamp == (time.Time{}) {
		entry.Timestamp = time.Now()
	}

	// Convert Entry to JSON
	entryJSON, err := json.Marshal(entry)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal Entry: %v", err)
	}

	// Convert JSON to byte slice
	entryBytes := []byte(entryJSON)

	position, numberOfBytesWritten, err := ms.writeEntry(filename, entryBytes)
	if err != nil {
		return 0, fmt.Errorf("failed to write entry: %v", err)
	}

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
		return 0, fmt.Errorf("failed to write index entry to file: %s, %v", indexFilename, err)
	}

	return offset, nil
}

// writeEntry writes an entry to the specified topic file
func (ms *MessageStore) writeEntry(filename string, entry []byte) (int64, int64, error) {

	ms.mu.Lock()
	defer ms.mu.Unlock()

	// Open the topic file for writing (append only)
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	// Get the current offset in the file
	fileInfo, err := file.Stat()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get file info: %v", err)
	}

	// Write the entry to the file
	numberOfBytesWritten, err := fmt.Fprintf(file, "%s\n", entry)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to write entry to index file: %v", err)
	}

	return fileInfo.Size(), int64(numberOfBytesWritten), nil
}

// ReadEntry reads an entry from the given offset from the specified topic
func (ms *MessageStore) ReadEntry(topic string, offset int64) (*Entry, error) {

	_, ok := ms.topics[topic]
	if !ok {
		//return nil, fmt.Errorf("topic '%s' not found", topic)
		ms.topics[topic] = topic + ".data"
	}
	filename, ok := ms.topics[topic]
	if !ok {
		return nil, fmt.Errorf("failed to find topic '%s'", topic)
	}

	// Get the index entry corresponding to the offset
	indexFilename := filename + ".idx"
	idx := newIndex(indexFilename)
	indexEntry, err := idx.getIndexEntry(offset)
	if err != nil {
		return nil, fmt.Errorf("failed to get index entry from file: %s, %v", indexFilename, err)
	}

	// Read the topic entry using the position and length from the index entry
	entryBytes, err := ms.readEntry(filename, indexEntry.Pos, indexEntry.Length)
	if err != nil {
		return nil, fmt.Errorf("failed to read entry from file: %v", err)
	}

	// Convert JSON slice of bytes to Entry
	var entry Entry
	if err := json.Unmarshal(entryBytes, &entry); err != nil {
		return nil, fmt.Errorf("failed to unmarshal Entry: %v", err)
	}

	return &entry, nil
}

// readEntry reads an entry from the specified topic file, at the specified offset
func (ms *MessageStore) readEntry(filename string, entryFileOffset, entryLengthInBytes int64) ([]byte, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	// Open the topic file for reading
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open topic file: %v", err)
	}
	defer file.Close()

	// Seek to the position of the entry in the topic file
	if _, err := file.Seek(entryFileOffset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to position %d in topic file: %v", entryFileOffset, err)
	}

	// Read the bytes corresponding to the length of the entry
	entryBytes := make([]byte, entryLengthInBytes)
	if _, err := file.Read(entryBytes); err != nil {
		return nil, fmt.Errorf("failed to read entry from topic file: %v", err)
	}

	return entryBytes, nil
}

// PollForNextEntry reads an entry from the given offset+1 from the specified topic, after sleeping for the specified poll interval
func (ms *MessageStore) PollForNextEntry(topic string, offset int64, pollDuration time.Duration) (*Entry, error) {

	_, ok := ms.topics[topic]
	if !ok {
		//return nil, fmt.Errorf("topic '%s' not found", topic)
		ms.topics[topic] = topic + ".data"
	}
	filename, ok := ms.topics[topic]
	if !ok {
		return nil, fmt.Errorf("failed to find topic '%s'", topic)
	}

	endTime := time.Now().Add(pollDuration)
	//entries := []*Entry{}

	for time.Now().Before(endTime) {
		// Wait for a short duration
		time.Sleep(51 * time.Millisecond)
	}

	indexFilename := filename + ".idx"
	idx := newIndex(indexFilename)
	count, maxoffset, err := idx.getMaxOffset()
	if err != nil {
		return nil, fmt.Errorf("failed to get getMaxOffset: %v", err)
	}

	// check if the topic is empty
	if count == 0 {
		return nil, nil
	}

	// check if the max offset in the topic index is greater than the given offset
	if maxoffset > offset {

		// calculate the next offset
		nextOffset := offset + 1

		// Read entry at the next offset
		entry, err := ms.ReadEntry(topic, nextOffset)
		if err != nil {
			return nil, fmt.Errorf("failed to read 'next' entry from topic %s at offset %d: %v", topic, nextOffset, err)
		}

		return entry, nil
	}

	return nil, nil
}
