package messagestore

import (
	"fmt"
	"testing"
	"time"
)

func TestSaveEntry(t *testing.T) {

	messageStore := NewMessageStore()
	topic := "topic1"
	err := messageStore.Delete(topic)
	if err != nil {
		t.Fatalf("Delete(), err: %+v", err)
	}
	entry1 := &Entry{
		Key:   nil,
		Value: []byte("test1"),
	}
	offset, err := messageStore.SaveEntry(topic, *entry1)
	if err != nil {
		t.Fatalf("SaveEntry(), entry:%+v, err: %+v", entry1, err)
	}
	if offset != 0 {
		t.Fatalf("SaveEntry(), got:%d, want: %d", offset, 0)
	}
}

func TestReadEntry(t *testing.T) {

	messageStore := NewMessageStore()
	topic := "topic1"
	err := messageStore.Delete(topic)
	if err != nil {
		t.Fatalf("Delete(), err: %+v", err)
	}
	//offset := int64(0)
	entry1 := &Entry{
		Key:   nil,
		Value: []byte("test1"),
	}
	offset, err := messageStore.SaveEntry(topic, *entry1)
	if err != nil {
		t.Fatalf("SaveEntry(), entry:%+v, err: %+v", entry1, err)
	}
	entry, err := messageStore.ReadEntry(topic, offset)
	//fmt.Println(entry.Value)
	fmt.Println(string(entry.Value))
	if err != nil {
		t.Fatalf("ReadEntry(), topic:%s, offset:%d, err: %+v", topic, offset, err)
	}
	if entry == nil {
		t.Fatalf("ReadEntry(), topic:%s, offset:%d, entry should not be nil ", topic, offset)
	}
	if entry != nil && string(entry.Key) != string(entry1.Key) {
		t.Fatalf("ReadEntry(), topic:%s, offset:%d, got:%v, want:%v ", topic, offset, entry.Key, entry1.Key)
	}
	if entry != nil && string(entry.Value) != string(entry1.Value) {
		t.Fatalf("ReadEntry(), topic:%s, offset:%d, got:%v, want:%v ", topic, offset, entry.Value, entry1.Value)
	}

	entry, err = messageStore.ReadEntry(topic, offset+1)
	if err == nil {
		t.Fatalf("ReadEntry(), topic:%s, offset:%d, err should not be nil", topic, offset)
	}
	if entry != nil {
		t.Fatalf("ReadEntry(), topic:%s, offset:%d, entry should be nil ", topic, offset)
	}
}

func TestReadEntryWhenMultipleEntriesExist(t *testing.T) {

	messageStore := NewMessageStore()
	topic := "topic1"
	err := messageStore.Delete(topic)
	if err != nil {
		t.Fatalf("Delete(), err: %v+", err)
	}
	//offset := int64(0)
	entry1 := &Entry{
		Key:   nil,
		Value: []byte("test1 the quick brown fox..."),
	}
	offset, err := messageStore.SaveEntry(topic, *entry1)
	if err != nil {
		t.Fatalf("SaveEntry(), entry:%+v, err: %v+", entry1, err)
	}
	if offset != 0 {
		t.Fatalf("SaveEntry(), topic:%s, offset:%d, want: %d", topic, offset, 0)
	}

	entry2 := &Entry{
		Key:   nil,
		Value: []byte("test2 red ball"),
	}
	offset, err = messageStore.SaveEntry(topic, *entry2)
	if err != nil {
		t.Fatalf("SaveEntry(), entry:%+v, err: %v+", entry2, err)
	}
	if offset != 1 {
		t.Fatalf("SaveEntry(), topic:%s, offset:%d, want: %d", topic, offset, 1)
	}

	entry3 := &Entry{
		Key:   nil,
		Value: []byte("test3 house"),
	}
	offset, err = messageStore.SaveEntry(topic, *entry3)
	if err != nil {
		t.Fatalf("SaveEntry(), entry:%v+, err: %v+", entry1, err)
	}
	if offset != 2 {
		t.Fatalf("SaveEntry(), topic:%s, offset:%d, want: %d", topic, offset, 2)
	}

	entry, err := messageStore.ReadEntry(topic, 1)
	//fmt.Println(entry.Value)
	fmt.Println(string(entry.Value))
	if err != nil {
		t.Fatalf("ReadEntry(), topic:%s, offset:%d, err: %+v", topic, offset, err)
	}
	if entry == nil {
		t.Fatalf("ReadEntry(), topic:%s, offset:%d, entry should not be nil ", topic, offset)
	}
	if entry != nil && string(entry.Key) != string(entry2.Key) {
		t.Fatalf("ReadEntry(), topic:%s, offset:%d, got:%v, want:%v ", topic, offset, entry.Key, entry2.Key)
	}
	if entry != nil && string(entry.Value) != string(entry2.Value) {
		t.Fatalf("ReadEntry(), topic:%s, offset:%d, got:%v, want:%v ", topic, offset, entry.Value, entry2.Value)
	}
	/*
		if !reflect.DeepEqual(entry, entry1) {
			t.Fatalf("ReadEntry(), topic:%s, offset:%d, got:%+v, want:%+v ", topic, offset, entry, entry1)
		}
	*/
}

func TestPollForNextEntryShouldReturnAnEntryBecauseTopicHasOneEntry(t *testing.T) {

	messageStore := NewMessageStore()
	topic := "topic1"
	err := messageStore.Delete(topic)
	if err != nil {
		t.Fatalf("Delete(), err: %+v", err)
	}
	//offset := int64(0)
	entry1 := &Entry{
		Key:   nil,
		Value: []byte("test1"),
	}
	offset, err := messageStore.SaveEntry(topic, *entry1)
	if err != nil {
		t.Fatalf("SaveEntry(), entry:%+v, err: %+v", entry1, err)
	}
	fmt.Println("SaveEntry returned offset: ", offset)

	fmt.Println("calling PollForNextEntry() should return entry, nil")
	fmt.Println(offset)
	offset = -1
	fmt.Println(offset)

	entry, err := messageStore.PollForNextEntry(topic, offset, 100*time.Millisecond)
	//fmt.Println(entry.Value)
	//fmt.Println(string(entry.Value))
	if err != nil {
		t.Fatalf("ReadEntry(), topic:%s, offset:%d, err: %+v", topic, offset, err)
	}
	if entry == nil {
		t.Fatalf("ReadEntry(), topic:%s, offset:%d, entry should not be nil ", topic, offset)
	}
	if entry != nil && string(entry.Key) != string(entry1.Key) {
		t.Fatalf("ReadEntry(), topic:%s, offset:%d, got:%v, want:%v ", topic, offset, entry.Key, entry1.Key)
	}
	if entry != nil && string(entry.Value) != string(entry1.Value) {
		t.Fatalf("ReadEntry(), topic:%s, offset:%d, got:%v, want:%v ", topic, offset, entry.Value, entry1.Value)
	}
}

func TestPollForNextEntryShouldNotReturnAnEntryOrAnErrorBecauseTopicIsEmpty(t *testing.T) {

	messageStore := NewMessageStore()
	topic := "topic1"
	err := messageStore.Delete(topic)
	if err != nil {
		t.Fatalf("Delete(), err: %+v", err)
	}
	offset := int64(0)
	fmt.Println("calling PollForNextEntry() should return nil, nil")

	entry, err := messageStore.PollForNextEntry(topic, offset, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("ReadEntry(), topic:%s, offset:%d, err: %+v", topic, offset, err)
	}
	if entry != nil {
		t.Fatalf("ReadEntry(), topic:%s, offset:%d, entry should be nil ", topic, offset)
	}
}
