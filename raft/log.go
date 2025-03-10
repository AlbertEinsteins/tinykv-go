// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"fmt"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries  since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	dummy     uint64
	dummyTerm uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	raftLog := &RaftLog{
		storage: storage,
	}

	// restore logs from storage
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	// Set dummy index and dummy term
	raftLog.dummy = firstIndex - 1
	term, err := storage.Term(raftLog.dummy)
	if err != nil {
		panic(err)
	}
	raftLog.dummyTerm = term

	// check if only exists the dummy log
	if firstIndex > lastIndex {
		// set state
		raftLog.stabled = lastIndex
		return raftLog
	}

	diskLogs, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		fmt.Println(firstIndex, lastIndex)
		panic(err)
	}
	// fmt.Println(firstIndex, lastIndex, diskLogs)
	raftLog.entries = append(raftLog.entries, diskLogs...)
	raftLog.stabled = lastIndex

	return raftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	if len(l.entries) == 0 {
		return
	}

	truncatedIndex, _ := l.storage.FirstIndex() // first is not compact log
	offset := l.entries[0].Index

	//delete entries [:truncatedIndex], excludes truncatedIndex
	if truncatedIndex >= offset {
		// dummyEnt := pb.Entry{Term: l.entries[0].Term, Index: l.entries[0].Index}

		remainEntries := l.entries[truncatedIndex-offset:]
		// fmt.Printf("truncate %d, offset %d, remain %v\n", truncatedIndex, offset,
		// 	remainEntries[:1])

		l.entries = []pb.Entry{}
		l.entries = append(l.entries, remainEntries...)
	}
	l.dummy = truncatedIndex - 1
	l.dummyTerm, _ = l.Term(l.dummy)
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries[:]
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return nil
	}

	lastIdx := l.entries[len(l.entries)-1].Index
	offset := l.entries[0].Index
	if l.stabled < lastIdx {
		return l.entries[l.stabled+1-offset:]
	}
	return []pb.Entry{}
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	if len(l.entries) == 0 || l.applied > l.committed {
		// fmt.Println(l.applied, l.committed)
		return nil
	}

	// Your Code Here (2A).
	offset := l.entries[0].Index

	// others, get from mem
	start := uint64(0)
	if l.applied >= offset {
		start = l.applied + 1 - offset
	}
	end := l.committed - offset

	// fmt.Println(offset, l.applied, l.committed, len(l.entries))
	ents = append(ents, l.entries[start:end+1]...)
	return ents
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	if len(l.entries) == 0 {
		// get from latest log in storage
		return l.dummy
	}

	// Your Code Here (2A).
	return l.entries[len(l.entries)-1].Index
}

func (l *RaftLog) FirstIndex() uint64 {
	if len(l.entries) == 0 {
		return l.dummy
	}
	return l.entries[0].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	if i == l.dummy {
		return l.dummyTerm, nil
	}

	// Your Code Here (2A).
	// check if in mem log
	if len(l.entries) != 0 &&
		(i >= l.entries[0].Index) &&
		(i-l.entries[0].Index) < uint64(len(l.entries)) {
		return l.entries[i-l.entries[0].Index].Term, nil
	}

	// then check disk
	return l.storage.Term(i)
}

func (l *RaftLog) LogRange(lo, hi uint64) []*pb.Entry {
	if len(l.entries) == 0 {
		return nil
	}

	offset := l.entries[0].Index
	if lo < offset || hi < offset {
		return nil
	}

	rtnEntries := make([]*pb.Entry, 0)
	end := min(hi-offset, uint64(len(l.entries)))
	// fmt.Println(l.entries[lo-offset : end])
	selected := l.entries[lo-offset : end]
	for idx := 0; idx < len(selected); idx++ {
		rtnEntries = append(rtnEntries, &selected[idx])
	}
	return rtnEntries
}

func (l *RaftLog) LogAt(idx uint64) *pb.Entry {
	logs := l.LogRange(idx, idx+1)

	if len(logs) == 0 {
		return nil
	}

	return logs[0]
}
