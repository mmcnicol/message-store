# message-store

a library which implements a traditional messaging store. 

text files are used for persistence.

a topic is persistence, which can contain entries.

an entry has a key and a value.

entries are written to a topic by appending to the topic text file.

entries are read from a topic by providing an offset.

entries in a topic cannot be queried.

entries in a topic are immutable, meaning they cannot be modified after they are written.

## usage

```
go get -v github.com/mmcnicol/message-store@v0.0.1
```

## public API

NewMessageStore() *MessageStore

SaveEntry(topic string, entry Entry) (int64, error)

ReadEntry(topic string, offset int64) (*Entry, error)

Delete(topic string)

## inspired by 

kafka
