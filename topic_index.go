package messagestore

import (
	"fmt"
	"io"
	"os"
	"sync"
)

// IndexEntry represents an entry in the index
type IndexEntry struct {
	Offset int64
	Pos    int64
	Length int64
}

// Index represents the index file
type Index struct {
	filename  string
	maxOffset int64
	mu        sync.Mutex
}

// NewIndex creates a new instance of Index
func newIndex(filename string) *Index {
	return &Index{
		filename: filename,
	}
}

// Delete will remove the specified topic index
func (idx *Index) delete() error {
	fmt.Println("in topic_index delete()")
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Check if the file exists
	if _, err := os.Stat(idx.filename); os.IsNotExist(err) {
		//fmt.Println("File does not exist")
	} else {
		//fmt.Println("File exists")
		// Delete the index file
		if err := os.Remove(idx.filename); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to delete index file: %s, %v", idx.filename, err)
		}
		fmt.Println("File deleted")
	}

	return nil
}

func (idx *Index) writeIndexEntry(entry IndexEntry) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	file, err := os.OpenFile(idx.filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open index file: %v", err)
	}
	defer file.Close()

	_, err = fmt.Fprintf(file, "%d %d %d\n", entry.Offset, entry.Pos, entry.Length)
	if err != nil {
		return fmt.Errorf("failed to write entry to index file: %v", err)
	}

	idx.maxOffset = entry.Offset

	return nil
}

// getIndexEntry returns an index entry for a given offset value
func (idx *Index) getIndexEntry(offset int64) (IndexEntry, error) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	file, err := os.Open(idx.filename)
	if err != nil {
		return IndexEntry{}, fmt.Errorf("failed to open index file: %v", err)
	}
	defer file.Close()

	var entry IndexEntry
	for {
		_, err := fmt.Fscanf(file, "%d %d %d\n", &entry.Offset, &entry.Pos, &entry.Length)
		if err != nil {
			if err == io.EOF {
				return IndexEntry{}, fmt.Errorf("index entry not found for offset %d", offset)
			}
			return IndexEntry{}, fmt.Errorf("failed to read index file: %v", err)
		}

		if entry.Offset == offset {
			return entry, nil
		}
	}
}

// getMaxOffset retrieves the maximum offset value from the index file
func (idx *Index) getMaxOffset() (int64, int64, error) {

	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Check if the file exists
	if _, err := os.Stat(idx.filename); os.IsNotExist(err) {
		//fmt.Println("File does not exist")
		return 0, 0, nil
	} else {
		//fmt.Println("File exists")
	}

	file, err := os.Open(idx.filename)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to open index file: %v", err)
	}
	defer file.Close()

	var count int64
	var offset int64
	var offsetFound bool

	var entry IndexEntry
	for {
		_, err := fmt.Fscanf(file, "%d %d %d\n", &entry.Offset, &entry.Pos, &entry.Length)
		if err != nil {
			break // End of file
		}

		count++
		offset = entry.Offset
		offsetFound = true
	}

	if !offsetFound {
		return 0, 0, fmt.Errorf("no offset found in index file")
	}

	return count, offset, nil
}

/*
// getIndexEntry returns an index entry for a given offset value
func (idx *Index) getIndexEntry(offset int64) (*IndexEntry, error) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	file, err := os.Open(idx.filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open index file: %v", err)
	}
	defer file.Close()

	//entries := make([]IndexEntry, 0)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) != 3 {
			return nil, fmt.Errorf("invalid entry in index file: %s", line)
		}

		entryOffset, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse offset: %v", err)
		}

		if entryOffset == offset {
			pos, err := strconv.ParseInt(fields[1], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse pos: %v", err)
			}

			length, err := strconv.ParseInt(fields[2], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse length: %v", err)
			}
			return &IndexEntry{Offset: offset, Pos: pos, Length: length}, nil
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading index file: %v", err)
	}

	return nil, fmt.Errorf("entry not found in index file for offset %d", offset)
}
*/

/*
// readIndexEntries reads all entries from the index file
func (idx *Index) readIndexEntries() ([]IndexEntry, error) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	file, err := os.Open(idx.filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open index file: %v", err)
	}
	defer file.Close()

	entries := make([]IndexEntry, 0)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) != 3 {
			return nil, fmt.Errorf("invalid entry in index file: %s", line)
		}

		offset, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse offset: %v", err)
		}

		pos, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse pos: %v", err)
		}

		length, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse length: %v", err)
		}

		entries = append(entries, IndexEntry{Offset: offset, Pos: pos, Length: length})
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading index file: %v", err)
	}

	return entries, nil
}
*/
