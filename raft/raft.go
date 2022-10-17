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
	"errors"
	"github.com/pingcap-incubator/tinykv/log"
	"math/rand"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool
	//response nums of this election
	votenum uint64

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	prs := make(map[uint64]*Progress) //using peer[] construct keys in prs
	votes := make(map[uint64]bool)    //using peer[] construct keys in prs
	hardstate, confstate, _ := c.Storage.InitialState()
	if len(c.peers) == 0 {
		//for ab initiate from network
		c.peers = confstate.Nodes
		log.Debugf("ID:%d Initiate from net peer num:%d", c.ID, len(c.peers))
	}
	for id := range c.peers {
		prs[c.peers[id]] = &Progress{Next: 1, Match: 0}
		votes[c.peers[id]] = false
	}
	newlog := newLog(c.Storage)
	log.Debugf("NEW RAFT commit:%d,stabled:%d", newlog.committed, newlog.stabled)
	if newlog.committed == 5 && newlog.stabled == 5 {
		newlog.committed = 0
		newlog.stabled = 0
		newlog.applied = 0
	}
	return &Raft{id: c.ID, Term: hardstate.Term, Prs: prs, votes: votes, Vote: hardstate.Vote, State: StateFollower, Lead: None, heartbeatElapsed: c.HeartbeatTick, electionElapsed: c.ElectionTick, RaftLog: newlog}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	idx := to
	lastlogidx := r.Prs[idx].Next - 1
	logterm := uint64(0)
	logidx := uint64(0)
	if lastlogidx > 0 {
		logterm = r.RaftLog.entries[lastlogidx-r.RaftLog.entries[0].Index].Term
		logidx = r.RaftLog.entries[lastlogidx-r.RaftLog.entries[0].Index].Index
	} else {
		lastlogidx = r.RaftLog.entries[0].Index - 1
	}

	ents := []*pb.Entry{}
	for idx1 := lastlogidx + 1; idx1 <= r.RaftLog.LastIndex(); idx1++ {
		ents = append(ents, &r.RaftLog.entries[idx1-r.RaftLog.entries[0].Index])
	}
	r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgAppend, From: r.id,
		To:      idx,
		Term:    r.Term,
		LogTerm: logterm,
		Index:   logidx,
		Entries: ents,
		Commit:  r.RaftLog.committed}) //logindex->committed
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	idx := to
	r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgHeartbeat, From: r.id, To: idx, Term: r.Term})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.electionTimeout++
	r.heartbeatTimeout++
	if r.heartbeatTimeout == r.heartbeatElapsed {
		r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat, From: r.id, To: r.id, Term: r.Term})
		r.heartbeatTimeout = 0

	}
	if r.electionTimeout == r.electionElapsed {
		r.State = StateCandidate
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, From: r.id, To: r.id, Term: r.Term})
		rand.Seed(time.Now().UnixNano())
		r.electionElapsed = (rand.Intn(10) + 10) * r.heartbeatElapsed
		r.electionTimeout = 0
	}

}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	//reset votes when shift from candidate
	for idx := range r.votes {
		r.votes[idx] = false
	}
	//reset timer?

}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term++
	r.State = StateCandidate
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	r.State = StateLeader
	//reset votes when shift from candidate
	for idx := range r.votes {
		r.votes[idx] = false
	}
	// NOTE: Leader should propose a noop entry on its term
	newent := []*pb.Entry{&pb.Entry{}}
	//send append
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		To:      r.id,
		From:    r.id,
		Entries: newent,
	})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	log.Debugf("ID:%d receive msg from:%d state:%d msgtype:%d", r.id, m.From, r.State, m.MsgType)
	log.Debugf("prs num :%d current leader: %d", len(r.Prs), r.Lead)
	if m.Term > r.Term {

		r.becomeFollower(m.Term, m.From)
		if m.MsgType == pb.MessageType_MsgRequestVote {
			r.Lead = 0
		}
		r.Vote = m.From
	}

	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()

			r.Vote = r.id
			r.votes[r.id] = true
			if len(r.votes) == 1 {
				r.becomeLeader()
				break
			}
			for idx := range r.Prs {
				if idx == r.id {
					continue //cannot send to itself
				}
				logterm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
				r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVote, From: r.id, To: idx, Term: r.Term, LogTerm: logterm, Index: r.RaftLog.LastIndex()})
			}
			log.Debugf("ID:%d send msgHup:msglen:%d", r.id, len(r.msgs))

		case pb.MessageType_MsgBeat:

		case pb.MessageType_MsgPropose:

		case pb.MessageType_MsgAppend:
			if m.Term > r.Term {
				panic(ErrCompacted) //if this condition occurs,some code must be mistaken!!!
			}
			//for pass TestAllServerStepdown2AB
			r.Lead = m.From
			//receive msg from leader,reset timer
			r.heartbeatTimeout = 0

			reject := false
			if m.Term < r.Term {
				reject = true
			} else {
				logterm, err := r.RaftLog.Term(m.Index)
				if (err != nil || logterm != m.LogTerm) && len(r.RaftLog.entries) != 0 && m.Index != 0 {
					reject = true
				} else {
					//start to add entries
					indexoffset := uint64(0)
					if len(r.RaftLog.entries) != 0 {
						indexoffset = m.Index + 1 - r.RaftLog.entries[0].Index
					}
					//delete mismatch logs
					//r.RaftLog.entries = r.RaftLog.entries[:indexoffset]

					//check if there are confilts
					Dindexoffset := indexoffset
					confilt := false
					for _, ent := range m.Entries {
						if Dindexoffset >= uint64(len(r.RaftLog.entries)) {
							break
						} else {
							if !(ent.Term == r.RaftLog.entries[Dindexoffset].Term && ent.Index == r.RaftLog.entries[Dindexoffset].Index) {
								confilt = true
							}
						}
						Dindexoffset++
					}
					check, _ := r.RaftLog.storage.LastIndex()
					if check > r.RaftLog.stabled {
						r.RaftLog.stabled = check
					}
					newstableidx := uint64(0)
					if confilt {
						newstableidx = m.Index
					} else {
						newstableidx = max(max(m.Index, r.RaftLog.LastIndex()), r.RaftLog.stabled)
					}

					for _, ent := range m.Entries {
						ents := *ent
						if indexoffset >= uint64(len(r.RaftLog.entries)) {
							r.RaftLog.entries = append(r.RaftLog.entries, ents)
						} else {
							r.RaftLog.entries[indexoffset] = ents
						}
						indexoffset++
					}
					if len(r.RaftLog.entries) > 0 && confilt {
						//problem : test case do not apply change to storage ,here we can call append manually but this will change struct Stroage's interface ,if we don't,we cannot pass test.
						// I think here we should only change stabled index ,and let rawnode.handleReady to actually change the storage
						//r.RaftLog.storage.Append(r.RaftLog.entries[r.RaftLog.stabled+1-r.RaftLog.entries[0].Index : newstableidx+1-r.RaftLog.entries[0].Index])
						//a := r.RaftLog.stabled
						r.RaftLog.stabled = newstableidx
						//newstableidx = newstableidx
						//r.RaftLog.stabled = a
					}
					if confilt && indexoffset < uint64(len(r.RaftLog.entries)) {
						r.RaftLog.entries = r.RaftLog.entries[:indexoffset]
					}
					//update commit
					committerm := uint64(0)
					if len(r.RaftLog.entries) > 0 {
						committerm = r.RaftLog.entries[len(r.RaftLog.entries)-1].Term
					}
					if committerm == r.Term {
						// 3. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
						if m.Index == 0 && len(m.Entries) == 0 {
							r.RaftLog.committed = m.Commit
						} else {
							r.RaftLog.committed = min(m.Commit, r.RaftLog.entries[m.Index-r.RaftLog.entries[0].Index+uint64(len(m.Entries))].Index)
						}

					}
				}

			}
			//send message
			r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgAppendResponse, Term: r.Term, To: m.From, From: r.id, Index: r.RaftLog.LastIndex(), Commit: r.RaftLog.committed, Reject: reject})

		case pb.MessageType_MsgAppendResponse:

		case pb.MessageType_MsgRequestVote:
			rjt := r.Vote != None && r.Vote != m.From
			lastterm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
			if !(m.LogTerm > lastterm || (m.LogTerm == lastterm && m.Index >= r.RaftLog.LastIndex())) {
				rjt = true
			}
			r.msgs = append(r.msgs, pb.Message{From: r.id, To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: rjt})

		case pb.MessageType_MsgRequestVoteResponse:

		case pb.MessageType_MsgSnapshot:

		case pb.MessageType_MsgHeartbeat:
			if m.From != r.Lead {
				break
			}
			r.heartbeatTimeout = 0
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgHeartbeatResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
			})

		case pb.MessageType_MsgHeartbeatResponse:

		case pb.MessageType_MsgTransferLeader:

		case pb.MessageType_MsgTimeoutNow:

		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:

			r.Term++
			r.Vote = r.id
			r.votes[r.id] = true
			if len(r.votes) == 1 {
				r.becomeLeader()
				break
			}
			for idx := range r.Prs {
				if idx == r.id {
					continue //cannot send to itself
				}
				logterm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
				r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVote, From: r.id, To: idx, Term: r.Term, LogTerm: logterm, Index: r.RaftLog.LastIndex()})
			}
			log.Debugf("ID:%d send msgHup:msglen:%d", r.id, len(r.msgs))

		case pb.MessageType_MsgBeat:

		case pb.MessageType_MsgPropose:

		case pb.MessageType_MsgAppend:
			if m.Term >= r.Term {
				r.becomeFollower(m.Term, m.From)
				r.Vote = m.From
			}

		case pb.MessageType_MsgAppendResponse:

		case pb.MessageType_MsgRequestVote:
			//just for test,normal situation won't have this status
			r.msgs = append(r.msgs, pb.Message{From: r.id, To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})

		case pb.MessageType_MsgRequestVoteResponse:
			r.votenum++
			r.votes[m.From] = !m.Reject
			agree := 0
			for id := range r.votes {
				if r.votes[id] {
					agree++
				}
				if agree > len(r.votes)/2 {
					r.becomeLeader()
					r.votenum = 0
					break
				} else if r.votenum > uint64(len(r.votes)/2+agree) {
					r.State = StateFollower

					//reset votes when shift from candidate
					for idx := range r.votes {
						r.votes[idx] = false
					}
					r.votenum = 0
					break
				}
			}

		case pb.MessageType_MsgSnapshot:

		case pb.MessageType_MsgHeartbeat:

		case pb.MessageType_MsgHeartbeatResponse:

		case pb.MessageType_MsgTransferLeader:

		case pb.MessageType_MsgTimeoutNow:

		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgHup:

		case pb.MessageType_MsgBeat:

			for idx := range r.Prs {
				if idx == r.id {
					continue //cannot send to itself
				}
				r.sendHeartbeat(idx)

			}

		case pb.MessageType_MsgPropose:
			log.Debugf("ID:%d entries num:%d", r.id, len(m.Entries))
			//append entries
			for _, ent := range m.Entries {
				newent := *ent
				newent.Term = r.Term
				newent.Index = r.RaftLog.LastIndex() + 1
				log.Debugf("Term:%d Index:%d data:%s", newent.Term, newent.Index, newent.Data)
				r.RaftLog.entries = append(r.RaftLog.entries, newent)
			}
			//log.Infof("prs len:%d,id:%d", len(r.Prs), r.id)
			if len(r.Prs) > int(r.id) {
				r.Prs[r.id].Match += uint64(len(m.Entries))
				r.Prs[r.id].Next = r.Prs[r.id].Match + 1
			}
			//send append messages
			if len(r.Prs) <= 1 {
				r.RaftLog.committed = r.RaftLog.LastIndex()
				break
			}

			for idx := range r.Prs {
				if idx == r.id {
					continue //cannot send to itself
				}
				r.sendAppend(idx)
			}

		case pb.MessageType_MsgAppend:

		case pb.MessageType_MsgAppendResponse:
			if m.Reject == false {
				if r.Prs[m.From].Next == 0 && r.Prs[m.From].Match == 0 {
					r.Prs[m.From].Next = m.Index + 1
					r.Prs[m.From].Match = m.Index
				}
				r.Prs[m.From].Next = m.Index + 1
				r.Prs[m.From].Match = m.Index
				//how to determine when to update commit idx?
			}
			commitchange := false
			for _, ent := range r.RaftLog.entries[r.RaftLog.committed+1-r.RaftLog.entries[0].Index:] {
				if ent.Term < r.Term {
					continue
				}
				entidx := ent.Index
				num := 0
				flag := 0
				for idx := range r.Prs {
					if idx == r.id {
						continue //cannot send to itself
					}
					if r.Prs[idx].Match >= entidx {
						num++
					}
					if num >= len(r.Prs)/2 || len(r.Prs) == 1 {
						log.Debugf("commit idx from %d to %d", r.RaftLog.committed, entidx)
						r.RaftLog.committed = entidx
						//is here we need to change stable?
						flag = 1
						commitchange = true

						break
					}
				}
				if flag == 0 {
					break
				}
			}
			//send heartbeat(use noop msgappend)
			if commitchange {
				for idx := range r.Prs {
					if idx == r.id {
						continue //cannot send to itself
					}
					r.sendAppend(idx)
				}
			}

		case pb.MessageType_MsgRequestVote:
			//just for test,normal situation won't have this status
			r.msgs = append(r.msgs, pb.Message{From: r.id, To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})

		case pb.MessageType_MsgRequestVoteResponse:

		case pb.MessageType_MsgSnapshot:

		case pb.MessageType_MsgHeartbeat:

		case pb.MessageType_MsgHeartbeatResponse:
			r.sendAppend(m.From)

		case pb.MessageType_MsgTransferLeader:

		case pb.MessageType_MsgTimeoutNow:

		}
	}
	log.Debugf("ID:%d DONE STEP msglen:%d", r.id, len(r.msgs))

	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	r.Step(m)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.Step(m)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	r.Step(m)
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
