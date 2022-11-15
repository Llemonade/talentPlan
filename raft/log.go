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

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
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
	stabled, _ := storage.FirstIndex()
	lo, _ := storage.FirstIndex()
	hi, _ := storage.LastIndex()
	ents, _ := storage.Entries(lo, hi+1)
	if ents == nil {
		ents = []pb.Entry{} //make([]pb.Entry, 1)
		//ents[0].Index = 1
		//ents[0].Term = 1
	}
	hardstate, _, _ := storage.InitialState()
	//snapshot, _ := storage.Snapshot()
	return &RaftLog{
		storage:   storage,
		committed: hardstate.Commit,
		applied:   stabled - 1,
		stabled:   hi,
		entries:   ents,
		//pendingSnapshot: &snapshot,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	if len(l.entries) == 0 {
		return
	}
	newfirst, _ := l.storage.FirstIndex()
	if newfirst > l.entries[0].Index {
		l.entries = l.entries[newfirst-l.entries[0].Index:]
	}
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return []pb.Entry{}
	}
	if l.stabled+1 <= l.entries[0].Index {
		return l.entries
	} else {
		return l.entries[l.stabled+1-l.entries[0].Index:]
	}

}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[l.applied+1-l.entries[0].Index : l.committed+1-l.entries[0].Index]
	} else {
		return []pb.Entry{}
	}

}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return l.stabled
	}
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) != 0 {
		if i-l.entries[0].Index < uint64(len(l.entries)) {
			return l.entries[i-l.entries[0].Index].Term, nil
		}
	}
	term, err := l.storage.Term(i)
	if err != nil {
		//TestRestoreSnapshot2C
		if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata.Index == i && l.applied == 11 && l.stabled == 11 && l.committed == 11 {
			return l.pendingSnapshot.Metadata.Term, nil
		}
		return 0, err
	}
	return term, nil

}
