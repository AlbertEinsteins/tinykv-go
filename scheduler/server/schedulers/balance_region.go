// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"fmt"
	"log"
	"sort"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"github.com/thoas/go-funk"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	maxStoreDownTime := cluster.GetMaxStoreDownTime()
	stores := cluster.GetStores()

	// 1.filter SuitableStore
	filterRes := funk.Filter(stores, func(storeInfo *core.StoreInfo) bool {
		return storeInfo.IsUp() && storeInfo.DownTime() < maxStoreDownTime
	})

	suitableStores, _ := filterRes.([]*core.StoreInfo)

	if len(suitableStores) < 2 {
		return nil
	}

	sort.Slice(suitableStores, func(i, j int) bool {
		return suitableStores[i].GetRegionSize() > suitableStores[j].GetRegionSize()
	})

	var fromStoreInfo, toStoreInfo *core.StoreInfo
	var regionInfo *core.RegionInfo

	for idx, storeInfo := range suitableStores {
		var regionCon core.RegionsContainer
		cluster.GetPendingRegionsWithLock(storeInfo.GetID(), func(rc core.RegionsContainer) {
			regionCon = rc
		})

		regionInfo = regionCon.RandomRegion(nil, nil)
		if regionInfo != nil {
			fromStoreInfo = suitableStores[idx]
			break
		}

		cluster.GetFollowersWithLock(storeInfo.GetID(), func(rc core.RegionsContainer) {
			regionCon = rc
		})
		regionInfo = regionCon.RandomRegion(nil, nil)
		if regionInfo != nil {
			fromStoreInfo = suitableStores[idx]
			break
		}

		cluster.GetLeadersWithLock(storeInfo.GetID(), func(rc core.RegionsContainer) {
			regionCon = rc
		})
		regionInfo = regionCon.RandomRegion(nil, nil)
		if regionInfo != nil {
			fromStoreInfo = suitableStores[idx]
			break
		}
	}

	if regionInfo == nil {
		return nil
	}

	storeIds := regionInfo.GetStoreIds()
	if len(storeIds) < cluster.GetMaxReplicas() { // do not need to
		return nil
	}

	// find toStore from store with lower region size
	for i := len(suitableStores) - 1; i >= 0; i-- {
		if _, ok := storeIds[suitableStores[i].GetID()]; !ok {
			toStoreInfo = suitableStores[i]
			break
		}
	}

	if toStoreInfo == nil {
		return nil
	}

	if fromStoreInfo.GetRegionSize()-toStoreInfo.GetRegionSize() < 2*regionInfo.GetApproximateSize() {
		return nil
	}

	peer, err := cluster.AllocPeer(toStoreInfo.GetID())
	if err != nil {
		log.Panic(err)
	}

	desc := fmt.Sprintf("move region [%d] from store {%d} to store [%d]", regionInfo.GetID(),
		fromStoreInfo.GetID(), toStoreInfo.GetID())
	op, err := operator.CreateMovePeerOperator(desc, cluster, regionInfo,
		operator.OpBalance, fromStoreInfo.GetID(), peer.StoreId, peer.Id)
	if err != nil {
		log.Panic(err)
	}
	return op
}
