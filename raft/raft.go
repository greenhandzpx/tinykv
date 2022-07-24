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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"sync"
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

	// all peers' id
	peers []uint64

	mu sync.Mutex

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

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

	log.SetLevel(log.LOG_LEVEL_DEBUG)

	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	raft := &Raft{
		id:    c.ID,
		Term:  uint64(0),
		peers: c.peers,
		Prs:   make(map[uint64]*Progress),
		State: StateFollower,

		votes: make(map[uint64]bool),

		Lead: uint64(0),

		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
	}
	for _, peer := range raft.peers {
		raft.Prs[peer] = &Progress{}
	}

	// init the log
	raftLog := newLog(c.Storage)
	raftLog.applied = c.Applied
	raft.RaftLog = raftLog
	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if r.RaftLog.LastIndex() < r.Prs[to].Next {
		log.Debugf("%v's log too short(lastIdx:%v), follower %v's nextIdx:%v",
			r.id, r.RaftLog.LastIndex(), to, r.Prs[to].Next)
		return false
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}
	logTerm, err := r.RaftLog.Term(r.Prs[to].Next - 1)
	if err != nil {
		// TODO
	}
	//if r.Prs[to].Match == r.Prs[to].Next {
	//	// means this follower needs a noop entry
	//	logTerm = 0
	//}
	msg.LogTerm = logTerm
	msg.Index = r.Prs[to].Next - 1
	//msg.PrevIndex = r.Prs[to].Match
	offset := r.RaftLog.entries[0].Index
	for i := r.Prs[to].Next - offset; i < uint64(len(r.RaftLog.entries)); i++ {
		msg.Entries = append(msg.Entries, &r.RaftLog.entries[i])
	}
	log.Debugf("%v send append entries(idx:%v) to %v",
		r.id, msg.Entries[0].Index, to)
	r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	// not sure whether we should lock here
	r.mu.Lock()
	defer r.mu.Unlock()

	r.electionElapsed++
	r.heartbeatElapsed++
	if r.electionElapsed == r.electionTimeout {
		log.Debugf("%v election timeout", r.id)
		msg := pb.Message{
			MsgType: pb.MessageType_MsgHup,
			To:      r.id,
			From:    r.id,
			Term:    r.Term,
		}
		//r.msgs = append(r.msgs, msg)
		r.mu.Unlock()
		err := r.Step(msg)
		if err != nil {
			// TODO: handle err
		}
		r.mu.Lock()
	}

	//if r.State != StateLeader {
	//	return
	//}
	if r.heartbeatElapsed == r.heartbeatTimeout {
		msg := pb.Message{
			MsgType: pb.MessageType_MsgBeat,
			To:      r.id,
			From:    r.id,
			Term:    r.Term,
		}
		//r.msgs = append(r.msgs, msg)
		r.mu.Unlock()
		err := r.Step(msg)
		if err != nil {
			// TODO: handle err
		}
		r.mu.Lock()
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.Lead = lead
	r.Term = term
	r.State = StateFollower
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	// not sure here
	r.Term++

	//msg := pb.Message{
	//	MsgType: pb.MessageType_MsgHup,
	//	To:      r.id,
	//	From:    r.id,
	//	Term:    r.Term,
	//}
	////r.msgs = append(r.msgs, msg)
	//r.election()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	log.Debugf("%v becomes leader", r.id)
	r.State = StateLeader
	r.Lead = r.id
	//msg := pb.Message{
	//	MsgType: pb.MessageType_MsgBeat,
	//	To:      r.id,
	//	From:    r.id,
	//	Term:    r.Term,
	//}
	// not sure here
	//r.msgs = append(r.msgs, msg)
	//r.heartbeat(msg)

	for _, peer := range r.peers {
		r.Prs[peer].Match = 0
		r.Prs[peer].Next = r.RaftLog.LastIndex() + 1
	}

	// append a noop entry
	// not sure here
	//if len(r.RaftLog.entries) == 0 {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		To:      r.id,
		From:    r.id,
		Term:    r.Term,
		Entries: []*pb.Entry{&pb.Entry{
			EntryType: pb.EntryType_EntryNormal,
			Term:      r.Term,
			Index:     r.RaftLog.LastIndex() + 1,
		}},
	}
	r.Step(msg)
	//}
}

func (r *Raft) resetElectionTimer() {
	r.electionElapsed = 0
	// not sure
	r.electionTimeout = int(rand.Int31n(10) + 10)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	//log.Debugf("%v(%v) get a msg, type:%v", r.id, r.State, m.MsgType)
	switch r.State {

	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			// election timeout, start a (new) election
			r.election()
		case pb.MessageType_MsgRequestVote:
			// get a request for voting
			r.handleRequestVote(m)
		case pb.MessageType_MsgHeartbeat:
			// get a heartbeat msg
			r.handleHeartbeat(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgPropose:
			// TODO
		default:

		}

	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			// election timeout, start a (new) election
			r.election()
		case pb.MessageType_MsgRequestVote:
			// get a request for voting
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleRequestVoteResponse(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		default:

		}

	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgRequestVote:
			// get a request for voting
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleRequestVoteResponse(m)
		case pb.MessageType_MsgBeat:
			r.heartbeat(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartbeatResponse(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendEntriesResponse(m)
		case pb.MessageType_MsgPropose:
			r.proposeAppendEntries(m)
		default:

		}
	}
	return nil
}

func (r *Raft) election() {
	// not sure whether we should lock the whole function or not
	r.mu.Lock()
	defer r.mu.Unlock()

	//log.Debugf("%v starts election", r.id)
	if r.State != StateCandidate {
		r.State = StateCandidate
	}
	// clear all msgs(not sure)
	r.msgs = make([]pb.Message, 0)
	r.Term++
	r.Vote = r.id
	r.resetElectionTimer()
	//log.Debugf("%v reset timer because of election, term:%v", r.id, r.Term)
	for _, peer := range r.peers {
		r.votes[peer] = false
	}
	r.votes[r.id] = true

	if len(r.peers) == 1 {
		// only one server
		r.becomeLeader()
		return
	}

	for _, peer := range r.peers {
		if peer == r.id {
			continue
		}
		msg := pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      peer,
			From:    r.id,
			Term:    r.Term,
		}
		lastIndex, err1 := r.RaftLog.storage.LastIndex()
		logTerm, err2 := r.RaftLog.storage.Term(lastIndex)
		if err1 != nil || err2 != nil {
			// TODO
		}
		msg.LogTerm = logTerm
		msg.Index = lastIndex
		//log.Debugf("send vote")
		r.msgs = append(r.msgs, msg)
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	r.mu.Lock()
	defer r.mu.Unlock()

	log.Debugf("%v request a vote from %v", m.From, r.id)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    m.Term,
		Reject:  true,
	}
	if r.Term > m.Term {
		msg.Term = r.Term
		r.msgs = append(r.msgs, msg)
		log.Debugf("%v(term:%v) can't vote for %v(term:%v)",
			r.id, r.Term, m.From, m.Term)
		return
	}

	if r.Term < m.Term {
		r.Term = m.Term
		r.State = StateFollower
		// not sure here
		r.Vote = 0
	}
	lastIndex := r.RaftLog.LastIndex()
	logTerm, err := r.RaftLog.Term(lastIndex)
	if err != nil {
		// TODO
	}
	// whether the candidate's log is up-to-date as the receiver's log
	var upToDate bool
	if logTerm > m.LogTerm {
		upToDate = false
	} else if logTerm < m.LogTerm {
		upToDate = true
	} else if lastIndex > m.Index {
		upToDate = false
	} else {
		upToDate = true
	}
	if !upToDate {
		log.Debugf("%v(t:%v,i:%v) can't vote for %v(t:%v,i:%v) for not up-to-date",
			r.id, logTerm, lastIndex, m.From, m.LogTerm, m.Index)
	}
	if r.Vote != 0 && r.Vote != m.From {
		log.Debugf("%v can't vote for %v, it has voted for %v",
			r.id, m.From, r.Vote)
	}
	if (r.Vote == 0 || r.Vote == m.From) && upToDate {
		msg.Reject = false
		r.Vote = m.From
		// when granting vote, reset timer
		r.resetElectionTimer()
		log.Debugf("%v reset timer because of granting", r.id)
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if m.Term > r.Term {
		// change to follower
		r.State = StateFollower
		r.Term = m.Term
		return
	}
	if m.Term < r.Term {
		// outdated vote
		return
	}
	if m.Reject == true {
		r.votes[m.From] = false
		return
	}
	log.Debugf("%v gets a vote from %v", r.id, m.From)
	r.votes[m.From] = true
	cnt := 0
	for _, granted := range r.votes {
		if granted {
			cnt++
		}
	}
	if cnt > len(r.peers)/2 {
		// change to leader
		r.becomeLeader()
		// TODO add a noop entry

	}
}

// heartbeat when a candidate change to leader,
// it starts to send heartbeat to other servers
func (r *Raft) heartbeat(m pb.Message) {
	r.mu.Lock()
	defer r.mu.Unlock()

	//if m.Term < r.Term {
	//	// outdated msg
	//	return
	//}

	for _, peer := range r.peers {
		if peer == r.id {
			continue
		}
		log.Debugf("%v send heartbeat to %v", r.id, peer)
		r.sendHeartbeat(peer)
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.mu.Lock()
	defer r.mu.Unlock()

	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    m.Term,
		Reject:  false,
	}
	if m.Term < r.Term {
		msg.Term = r.Term
		msg.Reject = true
		r.msgs = append(r.msgs, msg)
		return
	}

	r.resetElectionTimer()
	log.Debugf("%v reset timer because of heartbeat", r.id)

	r.becomeFollower(m.Term, m.From)

	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if m.Term > r.Term {
		r.becomeFollower(m.Term, 0)
	}
}

// proposeAppendEntries the client propose entries
func (r *Raft) proposeAppendEntries(m pb.Message) {
	r.mu.Lock()
	defer r.mu.Unlock()

	log.Debugf("%v get entries from client", r.id)

	//if len(r.RaftLog.entries) == 0 {
	//	// means this entry must be noop entry
	//	entry := m.Entries[0]
	//	entry.Term = r.Term
	//	entry.Index = 0
	//	r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	//	log.Debugf("%v get a noop entry(idx:%v) from client", r.id, m.Entries[0].Index)
	//	m.Entries = m.Entries[1:]
	//}

	// add the entries into the leader's own log
	for _, entry := range m.Entries {
		entry.Term = r.Term
		entry.Index = r.RaftLog.LastIndex() + 1
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	//r.RaftLog.storage.Append(r.RaftLog.unstableEntries())

	// forward these entries to followers
	// TODO
	for _, peer := range r.peers {
		if peer == r.id {
			continue
		}
		r.sendAppend(peer)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	r.mu.Lock()
	defer r.mu.Unlock()

	log.Debugf("%v get an append entry from %v", r.id, m.From)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    m.Term,
		Reject:  false,
	}
	if m.Term < r.Term {
		msg.Term = r.Term
		msg.Reject = true
		r.msgs = append(r.msgs, msg)
		return
	}

	r.resetElectionTimer()
	log.Debugf("%v reset timer because of appendEntries", r.id)
	r.becomeFollower(m.Term, m.From)

	// TODO: replicate logs
	//if m.Entries[0].Index == 0 {
	//	// noop entry
	//	// just append
	//	r.RaftLog.entries = []pb.Entry{*m.Entries[0]}
	//	//r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[0])
	//	//r.RaftLog.storage.Append(r.RaftLog.unstableEntries())
	//	msg.LogTerm = m.Entries[len(m.Entries)-1].Term
	//	msg.Index = m.Entries[len(m.Entries)-1].Index
	//	r.msgs = append(r.msgs, msg)
	//	log.Debugf("follower %v receive a noop entry", r.id)
	//	return
	//}

	match := false
	prevTerm, err := r.RaftLog.Term(m.Index)
	if err == ErrCompacted {
		match = true
	} else if err == ErrUnavailable {
		// maybe the follower's log is far behind the leader,
		// so we should tell the leader what our last index is to
		// let leader adjust the nextIndex quickly
		msg.Index = r.RaftLog.LastIndex()
	} else {
		if prevTerm == m.LogTerm {
			match = true
		} else {
			// the same index entry's term doesn't match
			// get the first index whose term is prevTerm
			// to speed up replicating
			offset := r.RaftLog.entries[0].Index
			for i := m.Index - offset; i > 0; i-- {
				msg.Index = r.RaftLog.entries[i].Index
				if r.RaftLog.entries[i].Term != prevTerm {
					break
				}
			}
		}
	}

	if !match {
		msg.Reject = true
		r.msgs = append(r.msgs, msg)
		return
	}

	offset := r.RaftLog.entries[0].Index
	for _, entry := range m.Entries {
		if entry.Index-offset >= uint64(len(r.RaftLog.entries)) {
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
			continue
		}
		if r.RaftLog.entries[entry.Index-offset].Term != entry.Term {
			r.RaftLog.entries[entry.Index-offset] = *entry
		}
	}
	// modify the commit index
	if m.Commit > r.RaftLog.committed {
		if m.Commit < r.RaftLog.LastIndex() {
			r.RaftLog.committed = m.Commit
		} else {
			r.RaftLog.committed = r.RaftLog.LastIndex()
		}
	}

	//r.RaftLog.storage.Append(r.RaftLog.unstableEntries())
	msg.LogTerm = m.Entries[len(m.Entries)-1].Term
	msg.Index = m.Entries[len(m.Entries)-1].Index
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if m.Term > r.Term {
		r.becomeFollower(m.Term, 0)
		return
	}
	if m.Term < r.Term {
		// outdated response
		return
	}

	//if m.Reject {
	//	r.Prs[m.From].Next = m.Index
	//	r.Prs[m.From].Match = m.Index - 1
	//	return
	//}

	log.Debugf("%v modify follower %v, nextIdx:%v", r.id, m.From, m.Index)
	r.Prs[m.From].Match = m.Index
	r.Prs[m.From].Next = m.Index + 1

	// check whether there exists any entry that can be committed
	// TODO
	find := false
	for i := len(r.RaftLog.entries) - 1; i >= 0; i-- {
		entry := r.RaftLog.entries[i]
		if entry.Term < r.Term {
			break
		}
		if entry.Index <= r.RaftLog.committed {
			break
		}
		cnt := 0
		for _, pr := range r.Prs {
			if pr.Match >= entry.Index {
				cnt++
			}
		}
		if cnt > len(r.peers)/2 {
			find = true
			r.RaftLog.committed = entry.Index
			break
		}
	}
	if find {
		// broadcast the followers to advance commit index
		// TODO
	}

}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
