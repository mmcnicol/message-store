# message-store

a library which implements a traditional messaging store. 

text files are used for persistence.

a topic is a event log, which has entries.

an entry has a key and a value.

entries in a topic cannot be queried.

entries in a topic is immutable, meaning it cannot be modified after it is written.

entries are read from a topic by providing an offset.
