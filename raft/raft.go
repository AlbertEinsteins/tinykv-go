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
	"fmt"
	"math/rand"
	"sort"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
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

	log.Debugf("init raft module, peers %v", c.peers)
	// Your Code Here (2A).

	raft := &Raft{
		id:               c.ID,
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		RaftLog:          newLog(c.Storage),
		peers:            c.peers,
	}

	// init state or restore
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}

	if len(c.peers) == 0 {
		raft.peers = confState.Nodes
	}

	// fmt.Println(hardState)
	raft.becomeFollower(hardState.Term, hardState.Vote)
	raft.RaftLog.committed = hardState.Commit

	// Restore applied from Config
	raft.RaftLog.applied = c.Applied
	raft.RaftLog.dummy = c.Applied
	raft.RaftLog.dummyTerm = hardState.Term

	log.Infof("raft-%d restart with last index %d, commited %d, applied %d",
		raft.id, raft.RaftLog.LastIndex(), raft.RaftLog.committed, raft.RaftLog.applied)
	raft.initProgress(raft.RaftLog.LastIndex() + 1)
	return raft
}

// softState return the current raft state
func (r *Raft) softState() *SoftState {
	return &SoftState{
		r.Lead,
		r.State,
	}
}

// softState return the current raft state
func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
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
	log.Debugf("node-%d start to singal elect, election ets %v", r.id, r.electionElapsed)

	electionStartMsg := pb.Message{
		MsgType: pb.MessageType_MsgHup,
		From:    r.id,
		To:      r.id,
	}
	r.Step(electionStartMsg)
}

func (r *Raft) triggerHearbeat() {
	heartbeatStartMsg := pb.Message{
		MsgType: pb.MessageType_MsgBeat,
		From:    r.id,
		To:      r.id,
	}
	r.Step(heartbeatStartMsg)
}

func (r *Raft) initProgress(nextIndex uint64) {
	r.Prs = make(map[uint64]*Progress)

	for _, peerId := range r.peers {
		r.Prs[peerId] = &Progress{
			Next:  nextIndex,
			Match: 0,
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	log.Debugf("node-[%d] leader [%d] in term {%d}", r.id, lead, term)
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = lead
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.electionElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++

	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	// log.Infof("node-[%d] turns to leader at term {%d}", r.id, r.Term)
	r.State = StateLeader
	r.Lead = r.id
	r.electionElapsed = 0

	// append no-op entry
	noopEntry := r.noOpEntry()
	noopEntry.Term = r.Term
	noopEntry.Index = r.RaftLog.LastIndex() + 1
	// fmt.Println("leader commit no-op entry, ", r.RaftLog.LastIndex())

	r.RaftLog.entries = append(r.RaftLog.entries, *noopEntry)
	r.initProgress(r.RaftLog.LastIndex())

	for _, peer := range r.peers {
		if peer == r.id {
			r.updateProcess(r.id, r.RaftLog.LastIndex(), r.RaftLog.LastIndex()+1)
		} else {
			r.updateProcess(r.id, 0, r.RaftLog.LastIndex()+1)
		}
	}
}

func (r *Raft) updateProcess(peer, nextMatch, nextNext uint64) {
	// log.Infof("node-[%d] update peer {%d} match idx {%d}, next {%d}", r.id, peer, nextMatch, nextNext)
	r.Prs[peer].Match = max(r.Prs[peer].Match, nextMatch)
	r.Prs[peer].Next = nextNext
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
	} else if m.MsgType == pb.MessageType_MsgHeartbeat {
		r.handleHeartbeat(m)
	} else if m.MsgType == pb.MessageType_MsgSnapshot {
		r.handleSnapshot(m)
	} else if m.MsgType == pb.MessageType_MsgTimeoutNow {
		r.handleMsgTimeout(m)
	} else if m.MsgType == pb.MessageType_MsgTransferLeader {
		r.handleLeaderTransfer(m)
	}

}

func (r *Raft) handleMsgLeader(m pb.Message) {
	if m.MsgType == pb.MessageType_MsgRequestVote {
		r.handleRequestVote(m)
	} else if m.MsgType == pb.MessageType_MsgAppend {
		r.handleAppendEntries(m)
	} else if m.MsgType == pb.MessageType_MsgAppendResponse {
		r.handleAppendEntriesResponse(m)
	} else if m.MsgType == pb.MessageType_MsgBeat { // local
		r.startHeartBeat()
	} else if m.MsgType == pb.MessageType_MsgPropose { //local
		r.handlePropose(m)
	} else if m.MsgType == pb.MessageType_MsgHeartbeat {
		r.handleHeartbeat(m)
	} else if m.MsgType == pb.MessageType_MsgHeartbeatResponse {
		r.handleHeartBeatResponse(m)
	} else if m.MsgType == pb.MessageType_MsgTransferLeader {
		r.handleLeaderTransfer(m)
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
	} else if m.MsgType == pb.MessageType_MsgSnapshot {
		r.handleSnapshot(m)
	} else if m.MsgType == pb.MessageType_MsgTimeoutNow {
		log.Infof("node-[%d] has been in candidate with term {%d}", r.id, r.Term)
	}
}

// ================ operation function ======================
func (r *Raft) handleMsgTimeout(m pb.Message) {
	fmt.Printf("node-[%d] in term {%d} receive a timeout msg from leader {%d}\n", r.id, r.Term, m.From)
	if m.Term < r.Term || !r.checkInGroup(r.id) {
		return
	}

	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
		To:      r.id,
	})
}

func (r *Raft) checkInGroup(peerId uint64) bool {
	for _, peer := range r.peers {
		if peer == peerId {
			return true
		}
	}
	return false
}

func (r *Raft) handleLeaderTransfer(m pb.Message) {
	// if m.Term < r.Term {
	// 	log.Warnf("node-[%d] receive a old leader transfer to {%d}", r.id, m.From)
	// 	return
	// }

	// leader check the transfee's qualification
	if r.State == StateLeader {
		// check m.from exists or transfer to self
		if _, ok := r.Prs[m.From]; !ok || m.From == r.id {
			return
		}
		// has been leader or in transfer process
		if r.leadTransferee != None {
			if r.leadTransferee == m.From {
				return
			}
			r.leadTransferee = None //shutdown last process
		}

		r.leadTransferee = m.From
		log.Infof("node-[%d] leader in term {%d} receive a leader transfer to {%d}", r.id, r.Term, m.From)

		if !r.checkQualification(m.From) {
			fmt.Println("not qualified, wait")
			r.sendAppend(m.From)
			return
		}

		fmt.Printf("leader [%d] send timeout msg to %d\n", r.id, m.From)
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgTimeoutNow,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
		})
	} else if r.State == StateFollower {
		// redirect leader transfer msg to leader if from is self
		if r.Lead != 0 {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	} else {
		fmt.Println("eRRRRRRRR")
	}
}

func (r *Raft) checkQualification(peer uint64) bool {
	peerMatch := r.Prs[peer].Match
	log.Infof("qualification peer-[%d], match {%d}, currrent leader {%d}, logs[:10] : %v",
		peer, peerMatch, r.RaftLog.LastIndex(), r.RaftLog.entries[:min(10, uint64(len(r.RaftLog.entries)))])
	return peerMatch == r.RaftLog.LastIndex()
}

func (r *Raft) handlePropose(m pb.Message) {
	log.Debugf("node-[%d] start to propose", r.id)
	// save local
	lastLogIdx := r.RaftLog.LastIndex()
	proposeEntries := make([]pb.Entry, 0)
	for idx, entry := range m.Entries {
		log := pb.Entry{
			Term:      r.Term,
			Index:     lastLogIdx + 1 + uint64(idx),
			Data:      entry.Data,
			EntryType: entry.EntryType,
		}
		proposeEntries = append(proposeEntries, log)
	}

	// update local Process
	r.appendLocally(proposeEntries)
	// start send
	r.updateProcess(r.id, r.RaftLog.LastIndex(), r.RaftLog.LastIndex()+1)

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

// Save Proposed Entries to Local Storage
// entries must be continues, and
// Note, entry.Index must be continues with entries in the storage before
func (r *Raft) appendLocally(entries []pb.Entry) {
	if len(entries) == 0 {
		return
	}
	r.RaftLog.entries = append(r.RaftLog.entries, entries...)
	// r.RaftLog.storage.AppendEntries(r.RaftLog.unstableEntries())
	// r.RaftLog.stabled = r.RaftLog.LastIndex()
}

func (r *Raft) startElection() {
	r.becomeCandidate()
	log.Infof("node-[%d] start to election in term {%d}", r.id, r.Term)

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
			// commit log entry
			r.updateCommitIndex()
		}
		return
	}

	latestCommitedIdx := r.RaftLog.committed
	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastLogIndex)

	for _, peerId := range r.peers {
		if peerId == r.id {
			continue
		}

		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			From:    r.id,
			To:      peerId,
			Term:    r.Term,
			LogTerm: lastLogTerm,
			Index:   lastLogIndex,
			Commit:  latestCommitedIdx,
		})
	}
}

func (r *Raft) startHeartBeat() {
	log.Debugf("node-[%d] start to send hearbeat in term {%d}", r.id, r.Term)

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
	// log.Infof("node-[%d] start to send ae to node-{%d} in term {%d}",
	// 	r.id, to, r.Term)

	r.send(to, pb.MessageType_MsgAppend)
	return true
}

func (r *Raft) send(to uint64, msgType pb.MessageType) {
	nextId := r.Prs[to].Next

	log.Debugf("node {%d}, send to {%d} nextIdx {%d}, first {%d}",
		r.id, to, nextId, r.RaftLog.FirstIndex())

	if nextId < r.RaftLog.FirstIndex() {
		r.sendSnapshot(to)
		return
	}

	entries := make([]*pb.Entry, 0)
	entries = append(entries, r.RaftLog.LogRange(nextId, r.RaftLog.LastIndex()+1)...)

	prevLogIdx := nextId - 1
	prevLogTerm, err := r.RaftLog.Term(prevLogIdx)
	// log.Infof("peer {%d}, send to {%d} prevLogIdx {%d}, prevlogterm {%d}", r.id, to, prevLogIdx, prevLogTerm)
	if err != nil {
		fmt.Println(prevLogIdx)
		panic(err)
	}

	msg := pb.Message{
		MsgType: msgType,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		LogTerm: prevLogTerm,
		Index:   prevLogIdx,
		Entries: entries,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
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
		Commit:  util.RaftInvalidIndex,
	})
}

func (r *Raft) sendSnapshot(to uint64) {
	// get snapshot
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		log.Warnf("node-[%d] snapshot is not ready.............", r.id)
		return
	}

	log.Infof("node-[%d] start to send to node-[%d] snapshot", r.id, to)
	r.msgs = append(r.msgs, pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Snapshot: &snapshot,
	})
}

// handle requestvote rpc request
func (r *Raft) handleRequestVote(m pb.Message) {
	// log.Infof("node-[%d] in term {%d} state {%v} receive a vote request in term {%v} from node {%d}",
	// 	r.id, r.Term, r.State.String(), m.Term, m.From)

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

	isVote := r.canVoteFor(m.From, m.LogTerm, m.Index)
	if isVote {
		r.Vote = m.From
		// log.Infof("node-[%d] vote for node-{%d}", r.id, m.From)
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  !isVote,
	})
}

func (r *Raft) canVoteFor(from, lastLogTerm, lastLogIndex uint64) bool {
	if r.Vote != 0 && r.Vote != from {
		return false
	}

	curLastLogIndex := r.RaftLog.LastIndex()
	curLastLogTerm, _ := r.RaftLog.Term(curLastLogIndex)

	log.Infof("node-[%d] <%d,%d>, candidate <%d,%d>", r.id, curLastLogIndex, curLastLogTerm,
		lastLogIndex, lastLogTerm)
	if lastLogTerm > curLastLogTerm ||
		(lastLogTerm == curLastLogTerm && lastLogIndex >= curLastLogIndex) {
		return true
	}
	return false
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	log.Debugf("node-[%d] receive a vote response from [%d], is vote {%v}", r.id, m.From, !m.Reject)
	if m.Term < r.Term { // old term request, ignore
		return
	}

	if m.Term > r.Term { // receive higher term response, turn to follower
		r.becomeFollower(m.Term, 0)
		return
	}

	// check node in group
	if !r.checkInGroup(r.id) {
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
		if voteCount > len(r.peers)/2 && r.State != StateLeader {
			r.becomeLeader()
			// commit no-op entry
			for _, peer := range r.peers {
				if peer == r.id {
					continue
				}

				r.sendAppend(peer)
			}
		}
		return
	}

	r.votes[m.From] = false

	// check votes, if a majority reject, from candidate turn to follower
	rejectCnt := 0
	for _, isVote := range r.votes {
		if !isVote {
			rejectCnt++
		}
	}

	if rejectCnt > len(r.peers)/2 {
		r.becomeFollower(m.Term, 0)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// log.Infof("node-[%d] in term {%d} receive a append msg in term {%d} from node-[%d], data %v, prevlog,prevterm <%d, %d>",
	// 	r.id, r.Term, m.Term, m.From, m.Entries, m.Index, m.LogTerm)

	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
		})
		return
	}

	r.becomeFollower(m.Term, m.From)
	if r.leadTransferee == m.From {
		log.Infof("node-[%d] receive a ae from {%d} set leadTransferee finished to 0", r.id, m.From)
		r.leadTransferee = 0
	}

	prevLogIndex := m.Index
	prevLogTerm := m.LogTerm
	log.Debugf("node-[%d] receive previdx %d, prevterm %d from node-[%d] cur first idx {%d}, dummy index {%d}, dummyTerm {%d}",
		r.id, prevLogIndex, prevLogTerm, m.From, r.RaftLog.FirstIndex(), r.RaftLog.dummy, r.RaftLog.dummyTerm)

	if prevLogIndex > r.RaftLog.LastIndex() { // case1. follower too short
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
			Index:   r.RaftLog.LastIndex(),
		})
		return
	}

	// case 2. prevlog is in snapshot
	if prevLogIndex < r.RaftLog.dummy {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
			Index:   r.RaftLog.LastIndex(),
		})
		return
	}

	if r.conflictAt(prevLogIndex, prevLogTerm) {
		// compute match index, rtn to leader
		// fmt.Printf("node-[%d] append conflict at idx {%d}\n", r.id, prevLogIndex)

		// TODO: find
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
			Index:   prevLogIndex,
		})
		return
	}

	r.processEntries(&m)
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Index:   m.Index + uint64(len(m.Entries)),
	})
}

func (r *Raft) processEntries(m *pb.Message) {

	// log.Infof("node-%d start to process entries %v, <%d, %d>", r.id, m.Entries, m.Index, m.LogTerm)
	idx := 0
	appendIdx := m.Index // get prevlogindex from msg
	for _, ent := range m.Entries {
		appendIdx++
		// log.Infof("ent {%d, %d}, cur logs %v", appendIdx, ent.Term, r.RaftLog.entries)
		if appendIdx <= r.RaftLog.LastIndex() {
			// fmt.Printf("node-[%d] append conflict at <%d>\n", r.id, appendIdx)

			term, err := r.RaftLog.Term(appendIdx)
			if err != nil {
				log.Panic(err)
			}
			if term != ent.Term {
				// delete [appendIdx:]
				log.Debugf("node-[%d] remove the entry after index {%d}", r.id, appendIdx)
				r.removeConflictEntryAfter(appendIdx)
				break
			} else {
				// exist same entry
				idx++
			}
		} else {
			break
		}
	}

	lastNewIdx := m.Index
	if len(m.Entries) > 0 {
		for _, ent := range m.Entries[idx:] {
			r.RaftLog.entries = append(r.RaftLog.entries, *ent)
		}

		// log.Infof("node-[%d] after append, current entries %v", r.id, r.RaftLog.entries)
		lastNewIdx = m.Entries[len(m.Entries)-1].Index
	}

	r.commitFollower(m.Commit, lastNewIdx)
}

// Delete Conflict Entries after given index
func (r *Raft) removeConflictEntryAfter(index uint64) {
	startPos := r.RaftLog.FirstIndex()
	r.RaftLog.stabled = min(r.RaftLog.stabled, index-1)
	r.RaftLog.entries = r.RaftLog.entries[:index-startPos]
}

func (r *Raft) commitFollower(leaderCommited, lastNewIdx uint64) {
	if leaderCommited > r.RaftLog.committed {
		r.RaftLog.committed = max(r.RaftLog.committed, min(leaderCommited, lastNewIdx))
		// log.Infof("node-[%d] in follower state commit index to {%d}, applied index {%d}", r.id, r.RaftLog.committed,
		// 	r.RaftLog.applied)
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
	log.Debugf("node-[%d] in term {%d} receive a append response in term {%d} from node-[%d]",
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
		matchIdx := max(r.Prs[m.From].Match, m.Index)
		r.updateProcess(m.From, matchIdx, matchIdx+1)
		r.updateCommitIndex()

		// TODO: check up-to-date, then transfer leader
		if r.leadTransferee == m.From && r.checkQualification(m.From) {
			// let transferee start elect
			log.Infof("node-[%d] help transferee {%d}, then send timeout to it", r.id, m.From)
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgTimeoutNow,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
			})
		}
	} else {
		// conflict entry, retry
		log.Infof("node-[%d] in leader state receive a conflict append response from node-{%d}", r.id, m.From)
		if r.Prs[m.From].Next >= r.RaftLog.FirstIndex() {
			r.Prs[m.From].Next--
		}
		r.sendAppend(m.From)
	}

}

func (r *Raft) updateCommitIndex() {
	if len(r.Prs) == 0 {
		return
	}

	matchCopy := make([]uint64, 0)
	for _, status := range r.Prs { // collect match index
		matchCopy = append(matchCopy, status.Match)
	}

	sort.Slice(matchCopy, func(i, j int) bool {
		return matchCopy[i] > matchCopy[j]
	})

	// fmt.Println(matchCopy)
	N := matchCopy[len(matchCopy)/2]

	if N > r.RaftLog.committed && r.RaftLog.LogAt(N).Term == r.Term {
		r.RaftLog.committed = N
		// log.Infof("node-[%d] after updated, current commit idx : %d", r.id, r.RaftLog.committed)

		// update follower commit idx
		for _, peer := range r.peers {
			if peer == r.id {
				continue
			}
			r.sendAppend(peer)
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	log.Debugf("node-[%d] in term {%d} receive a hearbeat msg in term {%d}, leader commit {%d}, cur logs {%d}, request %v",
		r.id, r.Term, m.Term, m.Commit, r.RaftLog.allEntries(), m)
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Commit:  r.RaftLog.committed,
		})
		return
	}

	r.becomeFollower(m.Term, m.From)

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	})
}

func (r *Raft) handleHeartBeatResponse(m pb.Message) {
	// log.Infof("node-[%d] receive a heartbeat response from node-[%d] in term {%d}", r.id, m.From, m.Term)
	if m.Term < r.Term {
		return
	}

	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		return
	}

	if !m.Reject {
		// update r.Prs
		// if follower's log is too short, need to replicate
		if r.RaftLog.committed > m.Commit {
			r.sendAppend(m.From)
		}
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	log.Infof("node-[%d] in term {%d} receive a snapshot request meta index {%d} in term {%d}, cur commited {%d}", r.id, r.Term, m.Snapshot.Metadata.Index, m.Term, r.RaftLog.committed)

	meta := m.Snapshot.Metadata
	if m.Term < r.Term {
		// drop
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Reject:  true,
			Index:   r.RaftLog.LastIndex(),
		})
		return
	}

	if r.RaftLog.committed >= meta.Index { // cur node commited, means old snapshot
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Reject:  true,
			Index:   r.RaftLog.LastIndex(),
		})
		return
	}

	log.Infof("node-[%d] handle a snapshot from {%d}", r.id, m.From)

	r.becomeFollower(m.Term, m.From)

	r.RaftLog.dummy = meta.Index
	r.RaftLog.dummyTerm = meta.Term

	r.RaftLog.applied = meta.Index
	r.RaftLog.committed = meta.Index
	r.RaftLog.stabled = meta.Index
	r.RaftLog.entries = make([]pb.Entry, 0)
	r.RaftLog.pendingSnapshot = m.Snapshot

	// init nodes
	r.peers = m.Snapshot.Metadata.ConfState.Nodes
	r.initProgress(r.RaftLog.LastIndex() + 1)

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Index:   meta.Index,
		Term:    r.Term,
	})
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	peers := r.peers

	// check exists
	for _, peer := range peers {
		if peer == id {
			return
		}
	}

	r.peers = append(r.peers, id)
	r.Prs[id] = &Progress{
		Match: 0,
		Next:  r.RaftLog.LastIndex() + 1,
	}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	peers := r.peers
	originSize := len(peers)

	start := 0
	for _, peer := range peers {
		if peer != id {
			peers[start] = peer
			start++
		}
	}

	r.peers = peers[:start]

	if start != originSize { // remove any element
		delete(r.Prs, id)
		// check if only exists one node
		r.updateCommitIndex()
	}

}
