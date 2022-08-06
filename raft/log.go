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
	"github.com/pingcap-incubator/tinykv/log"
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
	// the last compacted entry's term
	lastTerm uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	lastIndex, err1 := storage.LastIndex()
	firstIndex, err2 := storage.FirstIndex()
	if err1 != nil || err2 != nil {
		// TODO
	}

	// not sure
	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		// TODO
	}

	raftLog := &RaftLog{
		storage: storage,
		// not sure here
		//committed: lastIndex,
		//stabled:
		entries: entries,
	}
	return raftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	// TODO
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return l.entries
	}
	offset := l.entries[0].Index
	//log.Debugf("stabled idx:%v, offset:%v", l.stabled, offset)
	return l.entries[l.stabled+1-offset:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return l.entries
	}
	offset := l.entries[0].Index
	return l.entries[l.applied+1-offset : l.committed+1-offset]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		// TODO not sure
		return l.stabled
	}
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		log.Debugf("idx %v term %v, l.stabled %v", i, l.lastTerm, l.stabled)
		if i == l.stabled {
			return l.lastTerm, nil
		} else if i < l.stabled {
			return 0, ErrCompacted
		}
		return 0, ErrUnavailable
	}
	offset := l.entries[0].Index
	if i < offset {
		// TODO: maybe we cannot give the offset one?
		log.Debugf("ErrCompacted idx %v offset %v", i, offset)
		return l.lastTerm, ErrCompacted
		//return 0, ErrCompacted
	}
	if int(i-offset) >= len(l.entries) {
		log.Debugf("ErrUnavailable idx %v offset %v len(entries) %v",
			i, offset, len(l.entries))
		return 0, ErrUnavailable
	}
	return l.entries[i-offset].Term, nil
}
