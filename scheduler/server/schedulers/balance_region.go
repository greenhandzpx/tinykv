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
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
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
	storeInfos := cluster.GetStores()
	sort.SliceStable(storeInfos, func(i, j int) bool {
		return storeInfos[i].GetRegionSize() > storeInfos[j].GetRegionSize()
	})
	var regionInfo *core.RegionInfo
	var storeInfoMoved *core.StoreInfo
	for _, storeInfo := range storeInfos {
		cluster.GetPendingRegionsWithLock(storeInfo.GetID(), func(rc core.RegionsContainer) {
			if rc == nil {
				return
			}
			regionInfo = rc.RandomRegion(nil, nil)
		})
		if regionInfo != nil {
			storeInfoMoved = storeInfo
			break
		}
		cluster.GetFollowersWithLock(storeInfo.GetID(), func(rc core.RegionsContainer) {
			if rc == nil {
				return
			}
			regionInfo = rc.RandomRegion(nil, nil)
		})
		if regionInfo != nil {
			storeInfoMoved = storeInfo
			break
		}
		cluster.GetLeadersWithLock(storeInfo.GetID(), func(rc core.RegionsContainer) {
			if rc == nil {
				return
			}
			regionInfo = rc.RandomRegion(nil, nil)
		})
		if regionInfo != nil {
			storeInfoMoved = storeInfo
			break
		}
	}
	if regionInfo == nil {
		// cannot find region that should be removed
		log.Infof("cannot find region that should be removed")
		return nil
	}
	if len(regionInfo.GetStoreIds()) < cluster.GetMaxReplicas() {
		// TODO don't know why((
		return nil
	}
	// pick a store to move to
	storeInfosInRegion := cluster.GetRegionStores(regionInfo)
	var storeInfoToMove *core.StoreInfo
	for i := len(storeInfos) - 1; i >= 0; i-- {
		log.Infof("storeInfo %v", storeInfos[i].GetMeta())
		if storeInfos[i].DownTime() > cluster.GetMaxStoreDownTime() {
			// this store may be disconnected
			continue
		}
		exists := false
		for _, storeInfo := range storeInfosInRegion {
			log.Infof("storeInfo in region %v", storeInfo.GetMeta())
			if storeInfo.GetID() == storeInfos[i].GetID() {
				exists = true
				break
			}
		}
		if exists {
			// this store already has this region
			// so shouldn't be chosen as the one to move to
			continue
		}
		if storeInfoMoved.GetRegionSize()-storeInfos[i].GetRegionSize() >= regionInfo.GetApproximateSize()*2 {
			storeInfoToMove = storeInfos[i]
		} else {
			log.Infof("%v size not big enough; region size %v, old region size %v, region size %v",
				storeInfos[i].GetID(), storeInfos[i].GetRegionSize(), storeInfoMoved.GetRegionSize(), regionInfo.GetApproximateSize())
		}
		// else: the difference of size between this two stores is not big enough
		break
	}
	if storeInfoToMove == nil {
		// cannot find a store to move to
		log.Infof("cannot find a store to move to")
		return nil
	}
	var peerId uint64
	for _, peer := range regionInfo.GetPeers() {
		if peer.StoreId == storeInfoMoved.GetID() {
			peerId = peer.Id
			break
		}
	}
	op, err := operator.CreateMovePeerOperator("transfer a peer",
		cluster, regionInfo, operator.OpBalance, storeInfoMoved.GetID(), storeInfoToMove.GetID(), peerId)
	if err != nil {
		// TODO handle err
		log.Infof("err %v", err)
		return nil
	}
	log.Infof("op ret %v", op)
	return op
}
