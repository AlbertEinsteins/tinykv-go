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
	"math/rand"
	"sort"

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

	if len(c.peers) == 0 {
		return errors.New("peers must be a positive number")
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

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int

	randomElectionTimeout int
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

	peers []uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	raft := &Raft{
		id:               c.ID,
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		RaftLog:          newLog(c.Storage),
		State:            StateFollower,
		peers:            c.peers,
	}

	// init state
	raft.becomeFollower(0, 0)
	return raft
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed++

		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.triggerHearbeat()
		}
		return // block election
	}

	// random set election timeout
	r.electionElapsed++
	if r.electionElapsed >= r.randomElectionTimeout {
		r.triggerElection()
	}
}

func (r *Raft) triggerElection() {
	Debug(dLog, "node-%d start to singal elect, election ets %v\n", r.id, r.electionElapsed)

	electionStartMsg := pb.Message{
		MsgType: pb.MessageType_MsgHup,
		From:    r.id,
		To:      r.id,
	}
	r.Step(electionStartMsg)
}

func (r *Raft) triggerHearbeat() {
	Debug(dLog, "node-[%d] start to signal send heartbeat\n", r.id)
	heartbeatStartMsg := pb.Message{
		MsgType: pb.MessageType_MsgBeat,
		From:    r.id,
		To:      r.id,
	}
	r.Step(heartbeatStartMsg)
}

func (r *Raft) initProgress() {
	r.Prs = make(map[uint64]*Progress)

	for _, peerId := range r.peers {
		r.Prs[peerId] = &Progress{
			Next:  r.RaftLog.LastIndex() + 1,
			Match: 0,
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = lead
	r.randomElectionTimeout = 2*r.electionTimeout - rand.Intn(r.electionTimeout)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++

	r.electionElapsed = 0
	r.randomElectionTimeout = 2*r.electionTimeout - rand.Intn(r.electionTimeout)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	Debug(dLog, "node-[%d] turns to leader\n", r.id)
	r.State = StateLeader
	r.electionElapsed = 0

	r.initProgress()

	// append no-op entry
	noopEntry := r.noOpEntry()
	noopEntry.Term = r.Term
	noopEntry.Index = r.RaftLog.LastIndex() + 1

	r.RaftLog.entries = append(r.RaftLog.entries, *noopEntry)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.handleMsgFollower(m)
	case StateCandidate:
		r.handleMsgCandicate(m)
	case StateLeader:
		r.handleMsgLeader(m)
	}
	return nil
}

func (r *Raft) initVotes() {
	r.votes = make(map[uint64]bool)
	for _, peerId := range r.peers {
		r.votes[peerId] = false
	}
}

func (r *Raft) handleMsgFollower(m pb.Message) {
	if m.MsgType == pb.MessageType_MsgHup { // local msg
		r.startElection()
	} else if m.MsgType == pb.MessageType_MsgRequestVote { // remote msg
		r.handleRequestVote(m)
	} else if m.MsgType == pb.MessageType_MsgRequestVoteResponse {
		r.handleRequestVoteResponse(m)
	} else if m.MsgType == pb.MessageType_MsgAppend {
		r.handleAppendEntries(m)
	}
}

func (r *Raft) handleMsgLeader(m pb.Message) {
	if m.MsgType == pb.MessageType_MsgRequestVote {
		r.handleRequestVote(m)
	} else if m.MsgType == pb.MessageType_MsgAppend {
		r.handleAppendEntries(m)
	} else if m.MsgType == pb.MessageType_MsgAppendResponse {
		r.handleAppendEntriesResponse(m)
	} else if m.MsgType == pb.MessageType_MsgBeat {
		r.startHeartBeat()
	} else if m.MsgType == pb.MessageType_MsgPropose {
		r.handlePropose(m)
	}
}

func (r *Raft) handleMsgCandicate(m pb.Message) {
	if m.MsgType == pb.MessageType_MsgHup { // local msg
		r.startElection()
	} else if m.MsgType == pb.MessageType_MsgRequestVote {
		r.handleRequestVote(m)
	} else if m.MsgType == pb.MessageType_MsgRequestVoteResponse {
		r.handleRequestVoteResponse(m)
	} else if m.MsgType == pb.MessageType_MsgHeartbeat {
		r.handleHeartbeat(m)
	} else if m.MsgType == pb.MessageType_MsgAppend {
		r.handleAppendEntries(m)
	}
}

// ================ operation function ======================
func (r *Raft) handlePropose(m pb.Message) {
	Debug(dLog, "start to propose")

	// save local
	lastLogIdx := r.RaftLog.LastIndex()

	for idx, entry := range m.Entries {
		log := pb.Entry{
			Term:  r.Term,
			Index: lastLogIdx + 1 + uint64(idx),
			Data:  entry.Data,
		}

		r.RaftLog.entries = append(r.RaftLog.entries, log)
	}
	// update local Process
	r.Prs[r.id].Match = r.RaftLog.LastIndex()

	// check if only exists one node
	if len(r.peers) == 1 {
		r.updateCommitIndex()
		return
	}

	// then replicate to other peer
	for _, peerId := range r.peers {
		if peerId == r.id {
			continue
		}
		r.sendAppend(peerId)
	}
}

func (r *Raft) startElection() {
	r.becomeCandidate()
	Debug(dLog, "node-[%d] start to election in term {%d}", r.id, r.Term)

	r.initVotes()
	r.Vote = r.id
	r.votes[r.id] = true

	// check one node
	if len(r.peers) == 1 {
		voteCount := 0
		for _, isVote := range r.votes {
			if isVote {
				voteCount++
			}
		}
		if voteCount > len(r.votes)/2 && r.State != StateLeader {
			r.becomeLeader()
		}
		return
	}

	latestCommitedIdx := r.RaftLog.committed
	for _, peerId := range r.peers {
		if peerId == r.id {
			continue
		}

		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			From:    r.id,
			To:      peerId,
			Term:    r.Term,
			Commit:  latestCommitedIdx,
		})
	}
}

func (r *Raft) startHeartBeat() {
	Debug(dLog, "node-[%d] start to send hearbeat in term {%d}", r.id, r.Term+1)

	for _, peerId := range r.peers {
		if peerId == r.id {
			continue
		}

		r.sendHeartbeat(peerId)
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	Debug(dLog, "node-[%d] start to send ae to node-{%d} in term {%d}",
		r.id, to, r.Term)

	nextId := r.Prs[to].Next
	entries := r.RaftLog.LogRange(nextId, nextId+1)

	prevLogIdx := nextId - 1
	prevLogTerm, err := r.RaftLog.Term(prevLogIdx)
	if err != nil {
		panic(err)
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		LogTerm: prevLogTerm,
		Index:   prevLogIdx,
		Entries: entries,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

func (r *Raft) noOpEntry() *pb.Entry {
	noOpEntry := &pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Data:      nil,
	}
	return noOpEntry
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	})
}

// handle requestvote rpc request
func (r *Raft) handleRequestVote(m pb.Message) {
	Debug(dLog, "node-[%d] in term {%d} state {%v} receive a vote request in term {%v} from node {%d}",
		r.id, r.Term, r.State.String(), m.Term, m.From)

	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			From:    r.id,
			To:      m.From,
			Reject:  true,
			Term:    r.Term,
		})
		return
	}

	if m.Term > r.Term {
		r.becomeFollower(m.Term, 0)
		r.Vote = 0
		r.Term = m.Term
	}

	isVote := r.canVoteFor(m.From, m.Term, m.Commit)
	if isVote {
		r.Vote = m.From
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  !isVote,
	})
}

func (r *Raft) canVoteFor(from, term, latestCommitedIndex uint64) bool {
	if r.Vote != 0 && r.Vote != from {
		return false
	}
	if r.Term > term || r.RaftLog.committed < latestCommitedIndex {
		return false
	}
	return true
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	Debug(dLog, "node-[%d] receive a vote response from [%d], is vote {%v}", r.id, m.From, !m.Reject)

	if m.Term > r.Term { // receive higher term rpsponse
		r.becomeFollower(m.Term, m.From)
		return
	}

	if !m.Reject {
		r.votes[m.From] = true

		voteCount := 0
		for _, isVote := range r.votes {
			if isVote {
				voteCount++
			}
		}
		if voteCount > len(r.votes)/2 && r.State != StateLeader {
			r.becomeLeader()
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	Debug(dLog, "node-[%d] in term {%d} receive a append msg in term {%d} from node-[%d]", r.id, r.Term, m.Term, m.From)

	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
		})
		return
	}

	if m.Term >= r.Term {
		r.becomeFollower(m.Term, m.From)
	}

	prevLogIndex := m.LogTerm
	prevLogTerm := m.Index
	if r.conflictAt(prevLogIndex, prevLogTerm) {
		// compute match index, rtn to leader
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
			Index:   0,
		})
		return
	}

	r.processEntries(&m)
	r.commitFollower(m.Commit)
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Index:   m.Index + uint64(len(m.Entries)),
	})
}

func (r *Raft) processEntries(m *pb.Message) {
	// entries := m.Entries
	// TODO: append any new entries

	for _, ent := range m.Entries {
		r.RaftLog.entries = append(r.RaftLog.entries, *ent)
	}
}

func (r *Raft) commitFollower(leaderCommited uint64) {
	if leaderCommited > r.RaftLog.committed {
		r.RaftLog.committed = min(leaderCommited, r.RaftLog.LastIndex())
		Debug(dLog, "node-[%d] in follower commit index to {%d}", r.id, r.RaftLog.committed)
	}
}

func (r *Raft) conflictAt(prevLogIdx, prevLogTerm uint64) bool {
	if prevLogIdx > r.RaftLog.LastIndex() {
		return true
	}

	term, err := r.RaftLog.Term(prevLogIdx)
	if err != nil {
		panic(err)
	}

	return term != prevLogTerm
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	Debug(dLog, "node-[%d] in term {%d} receive a append response in term {%d} from node-[%d]",
		r.id, r.Term, m.Term, m.From)

	if m.Term < r.Term {
		return
	}

	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		return
	}

	if !m.Reject {
		// update r.Prs
		r.Prs[m.From].Match = max(r.Prs[m.From].Match, m.Index)
		r.Prs[m.From].Next = r.Prs[m.From].Match + 1
		r.updateCommitIndex()
	} else {
		// conflict entry
		Debug(dLog, "node-[%d] in leader state receive a conflict append response", r.id)
	}

}

func (r *Raft) updateCommitIndex() {
	matchCopy := make([]uint64, 0)
	for _, status := range r.Prs { // collect match index
		matchCopy = append(matchCopy, status.Match)
	}

	// fmt.Println(matchCopy)
	sort.Slice(matchCopy, func(i, j int) bool {
		return matchCopy[i] < matchCopy[j]
	})

	N := matchCopy[len(matchCopy)/2]

	if N > r.RaftLog.committed && r.RaftLog.LogAt(N).Term == r.Term {
		r.RaftLog.committed = max(r.RaftLog.committed, N)
		Debug(dLog, "node-[%d] after updated, current commit idx : %d", r.id, r.RaftLog.committed)
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	Debug(dLog, "node-[%d] in term {%d} receive a hearbeat msg in term {%d}", r.id, r.Term, m.Term)

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
