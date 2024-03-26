package main

import (
	"reflect"
	"testing"
)

func TestSaveEntry(t *testing.T) {

	messageStore := NewMessageStore()
	topic := "topic1"
	err := messageStore.Delete(topic)
	if err != nil {
		t.Fatalf("Delete(), err: %v+", err)
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
		t.Fatalf("Delete(), err: %v+", err)
	}
	//offset := int64(0)
	entry1 := &Entry{
		Key:   nil,
		Value: []byte("test1"),
	}
	offset, err := messageStore.SaveEntry(topic, *entry1)
	if err != nil {
		t.Fatalf("SaveEntry(), entry:%v+, err: %v+", entry1, err)
	}
	entry, err := messageStore.ReadEntry(topic, offset)
	if err != nil {
		t.Fatalf("ReadEntry(), topic:%s, offset:%d, err: %+v", topic, offset, err)
	}
	if !reflect.DeepEqual(entry, entry1) {
		t.Fatalf("ReadEntry(), topic:%s, offset:%d, got:%+v, want:%+v ", topic, offset, entry, entry1)
	}
}
