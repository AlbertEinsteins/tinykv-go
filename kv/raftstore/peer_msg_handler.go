package raftstore

import (
	"fmt"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx               *GlobalContext
	regionBeforeSplit metapb.Region
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if !d.RaftGroup.HasReady() {
		return
	}

	ready := d.RaftGroup.Ready()

	// fmt.Println(ready)
	// Save Locally, And Send

	applySnapResult, err := d.peer.peerStorage.SaveReadyState(&ready)
	if err != nil {
		log.Panic(err)
	}

	if applySnapResult != nil {
		d.updateRegionStateBySnap(applySnapResult)
	}

	d.peer.Send(d.ctx.trans, ready.Messages)

	// apply commited entries
	if len(ready.CommittedEntries) > 0 {
		writeBatch := &engine_util.WriteBatch{}

		for _, commitedEntry := range ready.CommittedEntries {
			// apply get, and collect put/delete op
			writeBatch = d.applyOrCollect(writeBatch, commitedEntry)

			if d.stopped {
				log.Warnf("peer-[%d] stop itself", d.PeerId())
				return
			}
		}

		// save to state machine
		// 1. set/update meta
		lastEnt := ready.CommittedEntries[len(ready.CommittedEntries)-1]
		d.peerStorage.applyState.AppliedIndex = lastEnt.Index
		if err = writeBatch.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState); err != nil {
			log.Panic(err)
		}

		// 2.write
		writeBatch.MustWriteToDB(d.ctx.engine.Kv)
	}

	d.RaftGroup.Advance(ready)
}

func (d *peerMsgHandler) handleProposal(ent *eraftpb.Entry, handler func(p *proposal)) {
	for len(d.proposals) > 0 {
		p := d.proposals[0]
		if ent.Term < p.term {
			return
		}

		if ent.Term > p.term {
			resp := ErrRespStaleCommand(ent.Term)
			p.cb.Done(resp)
			d.proposals = d.proposals[1:]
			continue
		}

		if ent.Index < p.index {
			return
		}

		if ent.Index > p.index {
			p.cb.Done(ErrRespStaleCommand(p.term))
			d.proposals = d.proposals[1:]
			continue
		}

		handler(p)
		d.proposals = d.proposals[1:]
	}
}

func (d *peerMsgHandler) applyOrCollect(wb *engine_util.WriteBatch, ent eraftpb.Entry) *engine_util.WriteBatch {
	if raft.IsNoopEntry(ent) {
		// fmt.Println("noop entry")
		return wb
	}

	if ent.EntryType == eraftpb.EntryType_EntryConfChange {
		confChange := eraftpb.ConfChange{}
		err := proto.Unmarshal(ent.Data, &confChange)
		if err != nil {
			log.Panic(err)
		}

		d.processConfChange(wb, &confChange, ent)
		return wb
	}

	raftCmd := raft_cmdpb.RaftCmdRequest{}
	err := proto.Unmarshal(ent.Data, &raftCmd)
	if err != nil {
		log.Panic(err)
	}

	// check region epoch
	if errEpochNotMatch, ok := util.CheckRegionEpoch(&raftCmd, d.Region(), true).(*util.ErrEpochNotMatch); ok {
		d.handleProposal(&ent, func(p *proposal) {
			p.cb.Done(ErrResp(errEpochNotMatch))
		})
		return wb
	}

	if raftCmd.AdminRequest != nil {
		d.processAdminRequest(wb, &raftCmd, ent)
		return wb
	}

	err = d.processRaftRequest(wb, &raftCmd, ent)
	if err != nil {
		d.handleProposal(&ent, func(p *proposal) {
			p.cb.Done(ErrRespStaleCommand(d.Term()))
		})
		return wb
	}

	d.handleProposal(&ent, func(p *proposal) {
		resp := raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
		}

		d.postProcessRaftProposal(p, &raftCmd, &resp)
		p.cb.Done(&resp)
	})
	return wb
}

func (d *peerMsgHandler) postProcessRaftProposal(p *proposal, raftCmd *raft_cmdpb.RaftCmdRequest,
	resp *raft_cmdpb.RaftCmdResponse) {
	for _, clientReq := range raftCmd.Requests {
		var clientResp raft_cmdpb.Response

		switch clientReq.CmdType {
		case raft_cmdpb.CmdType_Get:
			clientResp.CmdType = raft_cmdpb.CmdType_Get
			if err := util.CheckKeyInRegion(clientReq.Get.Key, d.Region()); err != nil {
				BindRespError(resp, err)
				return
			}

			val, err := engine_util.GetCF(d.ctx.engine.Kv, clientReq.Get.Cf, clientReq.Get.Key)
			if err != nil {
				log.Panic(err)
			}
			clientResp.Get = &raft_cmdpb.GetResponse{
				Value: val,
			}

		case raft_cmdpb.CmdType_Put:
			clientResp.CmdType = raft_cmdpb.CmdType_Put
			clientResp.Put = &raft_cmdpb.PutResponse{}
		case raft_cmdpb.CmdType_Delete:
			clientResp.CmdType = raft_cmdpb.CmdType_Delete
			clientResp.Delete = &raft_cmdpb.DeleteResponse{}
		case raft_cmdpb.CmdType_Snap:
			if errEpochNotMatch, ok := util.CheckRegionEpoch(raftCmd, d.Region(), true).(*util.ErrEpochNotMatch); ok {
				BindRespError(resp, errEpochNotMatch)
				return
			}

			clientResp.CmdType = raft_cmdpb.CmdType_Snap
			clientResp.Snap = &raft_cmdpb.SnapResponse{
				Region: d.Region(),
			}
			p.cb.Txn = d.ctx.engine.Kv.NewTransaction(false)
		}

		resp.Responses = append(resp.Responses, &clientResp)
	}
}

func (d *peerMsgHandler) processAdminRequest(KvWB *engine_util.WriteBatch, raftCmd *raft_cmdpb.RaftCmdRequest, ent eraftpb.Entry) {
	switch raftCmd.AdminRequest.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		compactReq := raftCmd.AdminRequest.CompactLog
		log.Warnf("peer-[%d] use schedule task to compact log, truncated idx include {%d}", d.PeerId(), compactReq.CompactIndex)

		if compactReq.CompactIndex > d.peerStorage.truncatedIndex() {
			truncatedState := d.peerStorage.applyState.TruncatedState
			truncatedState.Index = compactReq.CompactIndex
			truncatedState.Term = compactReq.CompactTerm

			d.scheduleSnapshotTask(compactReq.CompactIndex)
		}

		d.handleProposal(&ent, func(p *proposal) {
			p.cb.Done(&raft_cmdpb.RaftCmdResponse{
				AdminResponse: &raft_cmdpb.AdminResponse{
					CmdType:    raft_cmdpb.AdminCmdType_CompactLog,
					CompactLog: &raft_cmdpb.CompactLogResponse{},
				},
			})
		})

	case raft_cmdpb.AdminCmdType_Split:
		log.Infof("peer-[%d] start to handle split req, ent index %d", d.PeerId(), ent.Index)
		splitKey := raftCmd.AdminRequest.Split.SplitKey

		if raftCmd.Header.RegionId != d.regionId {
			d.handleProposal(&ent, func(p *proposal) {
				p.cb.Done(ErrRespRegionNotFound(raftCmd.Header.RegionId))
			})
			return
		}

		if errEpochNotMatch, ok := util.CheckRegionEpoch(raftCmd, d.Region(), true).(*util.ErrEpochNotMatch); ok {
			d.handleProposal(&ent, func(p *proposal) {
				p.cb.Done(ErrResp(errEpochNotMatch))
			})
			return
		}

		if err := util.CheckKeyInRegion(splitKey, d.Region()); err != nil {
			d.handleProposal(&ent, func(p *proposal) {
				p.cb.Done(ErrResp(err))
			})
			return
		}

		oldRegion, splitReq := d.Region(), raftCmd.AdminRequest.Split

		// split old region[S, E) to
		// old region [S, split), new region [split, E)
		newRegion := d.createSplitRegion(oldRegion, splitReq)

		// persist
		meta.WriteRegionState(KvWB, oldRegion, rspb.PeerState_Normal)
		meta.WriteRegionState(KvWB, newRegion, rspb.PeerState_Normal)

		// create peer
		peer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
		if err != nil {
			log.Panicf("store-[%d] create peer failed when split region %v", d.storeID(), d.regionId)
		}
		d.ctx.router.register(peer)
		d.ctx.router.send(newRegion.Id, message.Msg{Type: message.MsgTypeStart})

		d.updateMetaWithSplitRegion(oldRegion, d.peer, newRegion, peer)

		log.Infof("%v split old region %v, new region %v", d.Tag, oldRegion, newRegion)

		d.handleProposal(&ent, func(p *proposal) {
			p.cb.Done(&raft_cmdpb.RaftCmdResponse{
				Header: &raft_cmdpb.RaftResponseHeader{},
				AdminResponse: &raft_cmdpb.AdminResponse{
					CmdType: raft_cmdpb.AdminCmdType_Split,
					Split: &raft_cmdpb.SplitResponse{
						Regions: []*metapb.Region{newRegion, oldRegion},
					},
				},
			})
		})

		if d.IsLeader() {
			// update cache in scheduler
			d.notifyHeartbeatScheduler(oldRegion, d.peer)
			d.notifyHeartbeatScheduler(newRegion, peer)
		}
	}

}

func (d *peerMsgHandler) notifyHeartbeatScheduler(region *metapb.Region, peer *peer) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(region, clonedRegion)
	if err != nil {
		return
	}
	d.ctx.schedulerTaskSender <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            peer.Meta,
		PendingPeers:    peer.CollectPendingPeers(),
		ApproximateSize: peer.ApproximateSize,
	}
}

func (d *peerMsgHandler) createSplitRegion(oldRegion *metapb.Region, splitReq *raft_cmdpb.SplitRequest) *metapb.Region {
	oldRegionPeers := oldRegion.Peers
	region := &metapb.Region{
		RegionEpoch: &metapb.RegionEpoch{
			Version: InitEpochConfVer,
			ConfVer: InitEpochConfVer,
		},
	}

	if len(oldRegionPeers) != len(splitReq.NewPeerIds) {
		log.Panicf("not equals %v vs %v", oldRegionPeers, splitReq.NewPeerIds)
	}

	for _, peer := range oldRegionPeers { // copy old peers
		region.Peers = append(region.Peers, &metapb.Peer{
			Id:      peer.Id,
			StoreId: peer.StoreId,
		})
	}

	for idx, peerId := range splitReq.NewPeerIds {
		region.Peers[idx].Id = peerId
	}

	oldRegion.RegionEpoch.Version++
	region.RegionEpoch.Version++

	region.Id = splitReq.NewRegionId
	region.StartKey = splitReq.SplitKey
	region.EndKey = oldRegion.EndKey
	oldRegion.EndKey = splitReq.SplitKey

	return region
}

func (d *peerMsgHandler) updateMetaWithSplitRegion(oldRegion *metapb.Region, oldPeer *peer,
	newRegion *metapb.Region, newPeer *peer) {
	storeMeta := d.ctx.storeMeta
	storeMeta.Lock()
	defer storeMeta.Unlock()

	storeMeta.regionRanges.ReplaceOrInsert(&regionItem{oldRegion})
	storeMeta.regionRanges.ReplaceOrInsert(&regionItem{newRegion})

	storeMeta.setRegion(oldRegion, oldPeer)
	storeMeta.setRegion(newRegion, newPeer)
}

func (d *peerMsgHandler) processConfChange(kvWB *engine_util.WriteBatch, confChange *eraftpb.ConfChange, ent eraftpb.Entry) {

	var msg raft_cmdpb.RaftCmdRequest
	if err := msg.Unmarshal(confChange.Context); err != nil {
		log.Panic(err)
	}

	currentRegion := d.Region()

	if errorEpochNotMatch, ok := util.CheckRegionEpoch(&msg, currentRegion, true).(*util.ErrEpochNotMatch); ok {
		// log.Infof("")
		d.handleProposal(&ent, func(p *proposal) {
			p.cb.Done(ErrResp(errorEpochNotMatch))
		})
	}

	switch confChange.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		log.Infof("peer-[%d] in state {%v} add a peer {%d}", d.PeerId(), d.RaftGroup.Raft.State, confChange.NodeId)

		peerIdx := d.findPeerIndex(confChange.NodeId)
		if peerIdx < 0 { // Not exist
			readyPeer := msg.AdminRequest.ChangePeer.Peer
			currentRegion.Peers = append(currentRegion.Peers, readyPeer)
			currentRegion.RegionEpoch.ConfVer++
			// persist
			meta.WriteRegionState(kvWB, currentRegion, rspb.PeerState_Normal)

			// update region state
			d.updateRegionState(currentRegion)
			d.insertPeerCache(readyPeer)
		} else {
			log.Warnf("peer-[%d] add a peer [%d], but exists", d.PeerId(), currentRegion.Peers[peerIdx].Id)
		}

	case eraftpb.ConfChangeType_RemoveNode:
		log.Infof("peer-[%d] in state {%v} start to remove peer-[%d]", d.PeerId(), d.RaftGroup.Raft.State, confChange.NodeId)
		if confChange.NodeId == d.PeerId() {
			d.destroyPeer()
			return
		}

		peerIdx := d.findPeerIndex(confChange.NodeId)
		if peerIdx != -1 {
			currentRegion.RegionEpoch.ConfVer++
			currentRegion.Peers = append(currentRegion.Peers[:peerIdx], currentRegion.Peers[peerIdx+1:]...)

			meta.WriteRegionState(kvWB, currentRegion, rspb.PeerState_Normal)

			d.updateRegionState(currentRegion)
			d.removePeerCache(confChange.NodeId)
		}
	}

	d.RaftGroup.ApplyConfChange(*confChange)
	d.handleProposal(&ent, func(p *proposal) {
		raftResp := raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
				ChangePeer: &raft_cmdpb.ChangePeerResponse{Region: currentRegion},
			},
		}
		p.cb.Done(&raftResp)
	})

	if d.IsLeader() {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		d.notifyHeartbeatScheduler(d.Region(), d.peer)
	}
}

func (d *peerMsgHandler) updateRegionState(region *metapb.Region) {
	storeMeta := d.ctx.storeMeta
	storeMeta.Lock()
	defer storeMeta.Unlock()

	storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
	storeMeta.setRegion(region, d.peer)
}

func (d *peerMsgHandler) updateRegionStateBySnap(applySnapRes *ApplySnapResult) {
	storeMeta := d.ctx.storeMeta
	storeMeta.Lock()
	defer storeMeta.Unlock()

	storeMeta.regionRanges.Delete(&regionItem{applySnapRes.PrevRegion})

	storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: applySnapRes.Region})
	storeMeta.setRegion(applySnapRes.Region, d.peer)
}

func (d *peerMsgHandler) findPeerIndex(expectPeer uint64) int {
	region := d.Region()

	for idx, peer := range region.Peers {
		if peer.Id == expectPeer {
			return idx
		}
	}
	return -1
}

func (d *peerMsgHandler) processRaftRequest(wb *engine_util.WriteBatch, raftCmd *raft_cmdpb.RaftCmdRequest, ent eraftpb.Entry) error {

	for _, clientReq := range raftCmd.Requests {
		switch clientReq.CmdType {
		case raft_cmdpb.CmdType_Put:
			if err := util.CheckKeyInRegion(clientReq.Put.Key, d.Region()); err != nil {
				return err
			}

			putReq := clientReq.Put
			// append to writebacth
			wb.SetCF(putReq.Cf, putReq.Key, putReq.Value)
			d.SizeDiffHint += uint64(len(putReq.Key))
			d.SizeDiffHint += uint64(len(putReq.Value))

			// fmt.Printf("tag %v put key {%s}\n", d.Tag, putReq.Key)
		case raft_cmdpb.CmdType_Delete:
			if err := util.CheckKeyInRegion(clientReq.Delete.Key, d.Region()); err != nil {
				return err
			}

			delReq := clientReq.Delete
			// set val to nil, to delete key
			wb.SetCF(delReq.Cf, delReq.Key, nil)
			d.SizeDiffHint -= uint64(len(delReq.Key))
		}
	}

	return nil
}

func (d *peerMsgHandler) scheduleSnapshotTask(compactIndex uint64) {

	gcTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     compactIndex + 1,
	}
	d.LastCompactedIdx = gcTask.EndIdx
	d.ctx.raftLogGCTaskSender <- gcTask
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %s", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	// fmt.Println("propose, client type ", msg.Requests)
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	// if in leader transfer, reject request

	if d.RaftGroup.IsOnLeaderTransfer() {
		// log.Infof("leader is current in transfering state")
		errResp := ErrResp(errors.New("raftgroup is in leader transfering"))
		errResp.AdminResponse = &raft_cmdpb.AdminResponse{
			CmdType: msg.AdminRequest.CmdType,
		}
		cb.Done(errResp)
		return
	}
	// cache region
	d.regionBeforeSplit = *d.Region()

	if msg.AdminRequest != nil {
		d.proposeAdminMsg(msg, cb)
	} else {
		d.proposeMsg(msg, cb)
	}
}

func (d *peerMsgHandler) proposeAdminMsg(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	switch msg.AdminRequest.CmdType {
	case raft_cmdpb.AdminCmdType_InvalidAdmin:
		log.Warnf("peer-[%d] receive a invalid admin request", d.PeerId())
	case raft_cmdpb.AdminCmdType_CompactLog:
		d.proposeMsg(msg, cb)
	case raft_cmdpb.AdminCmdType_TransferLeader:
		// do not pass by the raft
		d.RaftGroup.TransferLeader(msg.AdminRequest.TransferLeader.Peer.Id)

		adminResp := raft_cmdpb.AdminResponse{
			CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
			TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
		}

		cb.Done(&raft_cmdpb.RaftCmdResponse{
			Header:        &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &adminResp,
		})
	case raft_cmdpb.AdminCmdType_ChangePeer:
		// must wait for last config update applied.
		// check region
		if errorEpochNotMatch, ok := util.CheckRegionEpoch(msg, d.Region(), true).(*util.ErrEpochNotMatch); ok {
			// log.Infof("")
			resp := &raft_cmdpb.RaftCmdResponse{}
			BindRespError(resp, errorEpochNotMatch)
			cb.Done(resp)
			return
		}

		if d.peerStorage.AppliedIndex() >= d.RaftGroup.Raft.PendingConfIndex {
			d.proposals = append(d.proposals, &proposal{
				index: d.nextProposalIndex(),
				term:  d.Term(),
				cb:    cb,
			})

			context, _ := msg.Marshal()
			d.RaftGroup.ProposeConfChange(eraftpb.ConfChange{
				ChangeType: msg.AdminRequest.ChangePeer.ChangeType,
				NodeId:     msg.AdminRequest.ChangePeer.Peer.Id,
				Context:    context,
			})
		}

	case raft_cmdpb.AdminCmdType_Split:
		// check req
		if err := util.CheckRegionEpoch(msg, d.Region(), true); err != nil {
			cb.Done(ErrResp(err))
			return
		}

		if err := util.CheckKeyInRegion(msg.AdminRequest.Split.SplitKey, d.Region()); err != nil {
			cb.Done(ErrResp(err))
			return
		}

		// log.Infof("%s start to propose split, propose index [%d]", d.Tag, d.nextProposalIndex())
		d.proposeMsg(msg, cb)
	}
}

func (d *peerMsgHandler) proposeMsg(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	d.proposals = append(d.proposals, &proposal{
		index: d.nextProposalIndex(),
		term:  d.Term(),
		cb:    cb,
	})

	// log.Infof("peer-%d process client msg %v", d.PeerId(), d.proposals)
	data, err := msg.Marshal()
	if err != nil {
		log.Panic(err)
	}
	err = d.RaftGroup.Propose(data)

	if err != nil {
		log.Panic(err)
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()

	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true

	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
