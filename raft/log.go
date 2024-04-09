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
	// storage contains all stable entries since the last snapshot.
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
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	log := &RaftLog{
		storage: storage,
	}
	return log
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).

	start, _ := l.storage.FirstIndex()
	end, _ := l.storage.LastIndex()

	logs, err := l.storage.Entries(start, end)

	if err != nil {
		panic("read entries all from storage err")
	}
	return logs
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
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	// get from storage
	ents, err := l.storage.Entries(l.applied+1, l.committed)
	if err != nil {
		panic(fmt.Sprintf("read entries [applied-{%d},commited-{%d}] from storage err",
			l.applied+1, l.committed))
	}
	return ents
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 { // get from storage
		lastIdx, err := l.storage.LastIndex()
		if err != nil {
			{
				panic("err read lastidx")
			}
		}
		return lastIdx
	}
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i <= l.stabled {
		term, err := l.storage.Term(i)
		if err != nil {
			panic(err)
		}
		return term, nil
	}

	// check entries
	if len(l.entries) == 0 {
		return 0, ErrUnavailable
	}
	offset := l.entries[0].Index
	if int(i-offset) >= len(l.entries) {
		return 0, ErrUnavailable
	}

	return l.entries[i-offset].Term, nil
}

func (l *RaftLog) LogRange(lo, hi uint64) []*pb.Entry {
	lastLogIdx, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}

	if lo > lastLogIdx { // means logs cur in memory if exists
		if len(l.entries) == 0 {
			return nil
		}

		offset := l.entries[0].Index
		end := min(uint64(len(l.entries)), hi-offset)
		logs := l.entries[lo-offset : end]
		entries := make([]*pb.Entry, 0)
		for _, log := range logs {
			entries = append(entries, &log)
		}
		return entries
	}

	logs, err := l.storage.Entries(lo, hi)
	if err != nil {
		panic(err)
	}

	entries := make([]*pb.Entry, len(logs))
	for idx, log := range logs {
		entries[idx] = &log
	}
	return entries
}

func (l *RaftLog) logAt(idx uint64) *pb.Entry {
	logs := l.LogRange(idx, idx+1)

	if len(logs) == 0 {
		return nil
	}

	return logs[0]
}
