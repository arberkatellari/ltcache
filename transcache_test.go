/*
TransCache is released under the MIT License <http://www.opensource.org/licenses/mit-license.php
Copyright (C) ITsysCOM GmbH. All Rights Reserved.
*/

package ltcache

import (
	"bufio"
	"bytes"
	"container/list"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRemKey(t *testing.T) {
	tc := NewTransCache(map[string]*CacheConfig{})
	tc.Set("t11_", "mm", "test", nil, true, "")
	if t1, ok := tc.Get("t11_", "mm"); !ok || t1 != "test" {
		t.Error("Error setting cache: ", ok, t1)
	}
	tc.Remove("t11_", "mm", true, "")
	if t1, ok := tc.Get("t11_", "mm"); ok || t1 == "test" {
		t.Error("Error removing cached key")
	}
}

func TestTransaction(t *testing.T) {
	tc := NewTransCache(map[string]*CacheConfig{})
	transID := tc.BeginTransaction()
	tc.Set("mmm_", "t11", "test", nil, false, transID)
	if t1, ok := tc.Get("mmm_", "t11"); ok || t1 == "test" {
		t.Error("Error in transaction cache")
	}
	tc.Set("mmm_", "t12", "test", nil, false, transID)
	tc.Remove("mmm_", "t11", false, transID)
	if _, hasTransID := tc.transactionBuffer[transID]; !hasTransID {
		t.Error("Does not have transactionID")
	}
	tc.CommitTransaction(transID)
	if t1, ok := tc.Get("mmm_", "t12"); !ok || t1 != "test" {
		t.Error("Error commiting transaction")
	}
	if t1, ok := tc.Get("mmm_", "t11"); ok || t1 == "test" {
		t.Error("Error in transaction cache")
	}
	if _, hasTransID := tc.transactionBuffer[transID]; hasTransID {
		t.Error("Should not longer have transactionID")
	}

}

func TestTransactionRemove(t *testing.T) {
	tc := NewTransCache(map[string]*CacheConfig{})
	transID := tc.BeginTransaction()
	tc.Set("t21_", "mm", "test", nil, false, transID)
	tc.Set("t21_", "nn", "test", nil, false, transID)
	tc.Remove("t21_", "mm", false, transID)
	if _, hasTransID := tc.transactionBuffer[transID]; !hasTransID {
		t.Error("Does not have transactionID")
	}
	tc.CommitTransaction(transID)
	if t1, ok := tc.Get("t21_", "mm"); ok || t1 == "test" {
		t.Error("Error commiting transaction")
	}
	if t1, ok := tc.Get("t21_", "nn"); !ok || t1 != "test" {
		t.Error("Error in transaction cache")
	}
	if _, hasTransID := tc.transactionBuffer[transID]; hasTransID {
		t.Error("Should not longer have transactionID")
	}
}

func TestTransactionRemoveGroup(t *testing.T) {
	tc := NewTransCache(map[string]*CacheConfig{})
	transID := tc.BeginTransaction()
	tc.Set("t21_", "mm", "test", []string{"grp1"}, false, transID)
	tc.Set("t21_", "nn", "test", []string{"grp1"}, false, transID)
	tc.RemoveGroup("t21_", "grp1", false, transID)
	if _, hasTransID := tc.transactionBuffer[transID]; !hasTransID {
		t.Error("Does not have transactionID")
	}
	tc.CommitTransaction(transID)
	if t1, ok := tc.Get("t21_", "mm"); ok || t1 == "test" {
		t.Error("Error commiting transaction")
	}
	if t1, ok := tc.Get("t21_", "nn"); ok || t1 == "test" {
		t.Error("Error in transaction cache")
	}
	if _, hasTransID := tc.transactionBuffer[transID]; hasTransID {
		t.Error("Should not longer have transactionID")
	}
}

func TestTransactionRollback(t *testing.T) {
	tc := NewTransCache(map[string]*CacheConfig{})
	transID := tc.BeginTransaction()
	tc.Set("aaa_", "t31", "test", nil, false, transID)
	if t1, ok := tc.Get("aaa_", "t31"); ok || t1 == "test" {
		t.Error("Error in transaction cache")
	}
	tc.Set("aaa_", "t32", "test", nil, false, transID)
	if _, hasTransID := tc.transactionBuffer[transID]; !hasTransID {
		t.Error("Does not have transactionID")
	}
	tc.RollbackTransaction(transID)
	if t1, ok := tc.Get("aaa_", "t32"); ok || t1 == "test" {
		t.Error("Error commiting transaction")
	}
	if t1, ok := tc.Get("aaa_", "t31"); ok || t1 == "test" {
		t.Error("Error in transaction cache")
	}
	if _, hasTransID := tc.transactionBuffer[transID]; hasTransID {
		t.Error("Should not longer have transactionID")
	}
}

func TestTransactionRemBefore(t *testing.T) {
	tc := NewTransCache(map[string]*CacheConfig{})
	transID := tc.BeginTransaction()
	tc.Remove("t41_", "mm", false, transID)
	tc.Remove("t41_", "nn", false, transID)
	tc.Set("t41_", "mm", "test", nil, false, transID)
	tc.Set("t41_", "nn", "test", nil, false, transID)
	tc.CommitTransaction(transID)
	if t1, ok := tc.Get("t41_", "mm"); !ok || t1 != "test" {
		t.Error("Error commiting transaction")
	}
	if t1, ok := tc.Get("t41_", "nn"); !ok || t1 != "test" {
		t.Error("Error in transaction cache")
	}
}

func TestTCGetGroupItems(t *testing.T) {
	tc := NewTransCache(map[string]*CacheConfig{})
	tc.Set("xxx_", "t1", "test", []string{"grp1"}, true, "")
	tc.Set("xxx_", "t2", "test", []string{"grp1"}, true, "")
	if grpItms := tc.GetGroupItems("xxx_", "grp1"); len(grpItms) != 2 {
		t.Errorf("Received group items: %+v", grpItms)
	}
	if grpItms := tc.GetGroupItems("xxx_", "nonexsitent"); grpItms != nil {
		t.Errorf("Received group items: %+v", grpItms)
	}
}

func TestRemGroup(t *testing.T) {
	tc := NewTransCache(map[string]*CacheConfig{})
	tc.Set("xxx_", "t1", "test", []string{"grp1"}, true, "")
	tc.Set("xxx_", "t2", "test", []string{"grp1"}, true, "")
	tc.RemoveGroup("xxx_", "grp1", true, "")
	_, okt1 := tc.Get("xxx_", "t1")
	_, okt2 := tc.Get("xxx_", "t2")
	if okt1 || okt2 {
		t.Error("Error removing prefix: ", okt1, okt2)
	}
}

func TestCacheCount(t *testing.T) {
	tc := NewTransCache(map[string]*CacheConfig{
		"dst_": {MaxItems: -1},
		"rpf_": {MaxItems: -1}})
	tc.Set("dst_", "A1", "1", nil, true, "")
	tc.Set("dst_", "A2", "2", nil, true, "")
	tc.Set("rpf_", "A3", "3", nil, true, "")
	tc.Set("dst_", "A4", "4", nil, true, "")
	tc.Set("dst_", "A5", "5", nil, true, "")
	if itms := tc.GetItemIDs("dst_", ""); len(itms) != 4 {
		t.Errorf("Error getting item ids: %+v", itms)
	}
}

func TestCacheGetStats(t *testing.T) {
	tc := NewTransCache(map[string]*CacheConfig{
		"part1": {MaxItems: -1},
		"part2": {MaxItems: -1}})
	testCIs := []*cachedItem{
		{itemID: "_1_", value: "one"},
		{itemID: "_2_", value: "two", groupIDs: []string{"grp1"}},
		{itemID: "_3_", value: "three", groupIDs: []string{"grp1", "grp2"}},
		{itemID: "_4_", value: "four", groupIDs: []string{"grp1", "grp2", "grp3"}},
		{itemID: "_5_", value: "five", groupIDs: []string{"grp4"}},
	}
	for _, ci := range testCIs {
		tc.Set("part1", ci.itemID, ci.value, ci.groupIDs, true, "")
	}
	for _, ci := range testCIs[:4] {
		tc.Set("part2", ci.itemID, ci.value, ci.groupIDs, true, "")
	}
	eCs := map[string]*CacheStats{
		"part1": {Items: 5, Groups: 4},
		"part2": {Items: 4, Groups: 3},
	}
	if cs := tc.GetCacheStats(nil); reflect.DeepEqual(eCs, cs) {
		t.Errorf("expecting: %+v, received: %+v", eCs, cs)
	}
}

// Try concurrent read/write of the cache
func TestCacheConcurrent(t *testing.T) {
	tc := NewTransCache(map[string]*CacheConfig{
		"dst_": {MaxItems: -1},
		"rpf_": {MaxItems: -1}})
	s := &struct{ Prefix string }{Prefix: "+49"}
	tc.Set("dst_", "DE", s, nil, true, "")
	wg := new(sync.WaitGroup)
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			tc.Get("dst_", "DE")
			wg.Done()
		}()
	}
	s.Prefix = "+491"
	wg.Wait()
}

type TenantID struct {
	Tenant string
	ID     string
}

func (tID *TenantID) Clone() (interface{}, error) {
	tClone := new(TenantID)
	*tClone = *tID
	return tClone, nil
}

func TestGetClone(t *testing.T) {
	tc := NewTransCache(map[string]*CacheConfig{})
	a := &TenantID{Tenant: "cgrates.org", ID: "ID#1"}
	tc.Set("t11_", "mm", a, nil, true, "")
	if t1, ok := tc.Get("t11_", "mm"); !ok {
		t.Error("Error setting cache: ", ok, t1)
	}
	if x, err := tc.GetCloned("t11_", "mm"); err != nil {
		t.Error(err)
	} else {
		tcCloned := x.(*TenantID)
		if !reflect.DeepEqual(tcCloned, a) {
			t.Errorf("Expecting: %+v, received: %+v", a, tcCloned)
		}
		a.ID = "ID#2"
		if reflect.DeepEqual(tcCloned, a) {
			t.Errorf("Expecting: %+v, received: %+v", a, tcCloned)
		}
	}
}

func TestGetClone2(t *testing.T) {
	tc := NewTransCache(map[string]*CacheConfig{})
	tc.Set("t11_", "mm", nil, nil, true, "")
	if x, err := tc.GetCloned("t11_", "mm"); err != nil {
		t.Error(err)
	} else if x != nil {
		t.Errorf("Expecting: nil, received: %+v", x)
	}
}

func TestTranscacheHasGroup(t *testing.T) {
	tc := &TransCache{
		cache: map[string]*Cache{
			"testChID": {},
		},
	}
	chID := "testChID"
	grpID := "testGroupID"

	exp := false
	rcv := tc.HasGroup(chID, grpID)

	if rcv != exp {
		t.Errorf("\nexpected: <%+v>, \nreceived: <%+v>", exp, rcv)
	}

	tc.cache[chID].groups = map[string]map[string]struct{}{
		"testGroupID": make(map[string]struct{}),
	}

	exp = true
	rcv = tc.HasGroup(chID, grpID)

	if rcv != exp {
		t.Errorf("\nexpected: <%+v>, \nreceived: <%+v>", exp, rcv)
	}
}

func TestTranscacheGetGroupItemIDs(t *testing.T) {
	tc := &TransCache{
		cache: map[string]*Cache{
			"testChID": {
				groups: map[string]map[string]struct{}{
					"testGroupID": {
						"testField1": struct{}{},
						"testField2": struct{}{},
					},
				},
			},
		},
	}
	chID := "testChID"
	grpID := "testGroupID"

	exp := []string{"testField1", "testField2"}
	rcv := tc.GetGroupItemIDs(chID, grpID)

	if len(exp) != len(rcv) {
		t.Fatalf("\nexpected slice length: <%+v>, \nreceived slice length: <%+v>",
			len(exp), len(rcv))
	}

	diff := make(map[string]int, len(exp))

	for _, valRcv := range rcv {
		diff[valRcv]++
	}
	for _, valExp := range exp {
		if _, ok := diff[valExp]; !ok {
			t.Fatalf("\nexpected: <%+v>, \nreceived: <%+v>", exp, rcv)
		}
		diff[valExp] -= 1
		if diff[valExp] == 0 {
			delete(diff, valExp)
		}
	}
	if len(diff) != 0 {
		t.Errorf("\nexpected: <%+v>, \nreceived: <%+v>", exp, rcv)
	}
}

func TestTranscacheClearSpecific(t *testing.T) {
	tc := &TransCache{
		cache: map[string]*Cache{
			"testChID1": {
				lruIdx: list.New(),
				ttlIdx: list.New(),
				cache: map[string]*cachedItem{
					"item1": {},
					"item2": {},
				},
			},
			"testChID2": {
				lruIdx: list.New(),
				ttlIdx: list.New(),
				cache: map[string]*cachedItem{
					"item1": {},
					"item2": {},
					"item3": {},
				},
			},
		},
	}
	chIDs := []string{"testChID2"}

	exp := &TransCache{
		cache: map[string]*Cache{
			"testChID1": {
				lruIdx: list.New(),
				ttlIdx: list.New(),
				cache: map[string]*cachedItem{
					"item1": {},
					"item2": {},
				},
			},
			"testChID2": {
				lruIdx: list.New(),
				ttlIdx: list.New(),
				cache:  map[string]*cachedItem{},
			},
		},
	}

	tc.Clear(chIDs)

	for key, value := range tc.cache {
		if len(value.cache) != len(exp.cache[key].cache) {
			t.Errorf("\nKey: <%+v>\nexpected nr of items: <%+v>, \nreceived nr of items: <%+v>",
				key, len(exp.cache[key].cache), len(value.cache))
		}
	}
}

func TestTranscacheClearAll(t *testing.T) {
	tc := &TransCache{
		cache: map[string]*Cache{
			"testChID1": {
				lruIdx: list.New(),
				ttlIdx: list.New(),
				cache: map[string]*cachedItem{
					"item1": {},
					"item2": {},
				},
			},
			"testChID2": {
				lruIdx: list.New(),
				ttlIdx: list.New(),
				cache: map[string]*cachedItem{
					"item1": {},
					"item2": {},
					"item3": {},
				},
			},
		},
	}
	var chIDs []string

	exp := &TransCache{
		cache: map[string]*Cache{
			"testChID1": {
				lruIdx: list.New(),
				ttlIdx: list.New(),
				cache:  map[string]*cachedItem{},
			},
			"testChID2": {
				lruIdx: list.New(),
				ttlIdx: list.New(),
				cache:  map[string]*cachedItem{},
			},
		},
	}

	tc.Clear(chIDs)

	for key, value := range tc.cache {
		if len(value.cache) != len(exp.cache[key].cache) {
			t.Errorf("\nKey: <%+v>\nexpected nr of items: <%+v>, \nreceived nr of items: <%+v>",
				key, len(exp.cache[key].cache), len(value.cache))
		}
	}
}

func TestTranscacheClearWithOfflineCollector(t *testing.T) {
	var logBuf bytes.Buffer
	tc := &TransCache{
		cache: map[string]*Cache{
			"testChID1": {
				lruIdx: list.New(),
				ttlIdx: list.New(),
				cache: map[string]*cachedItem{
					"item1": {itemID: "item1"},
					"item2": {itemID: "item2"},
				},
				offCollector: &OfflineCollector{
					logger: &testLogger{log.New(&logBuf, "", 0)},
					collection: map[string]*CollectionEntity{
						"item1": {IsSet: true, ItemID: "item1"},
						"item2": {IsSet: true, ItemID: "item2"},
						"item3": {IsSet: false, ItemID: "item3"},
					},
				},
			},
			"testChID2": {
				lruIdx: list.New(),
				ttlIdx: list.New(),
				cache: map[string]*cachedItem{
					"item1": {itemID: "item1"},
					"item2": {itemID: "item2"},
					"item3": {itemID: "item3"},
				},
				offCollector: &OfflineCollector{
					logger: &testLogger{log.New(&logBuf, "", 0)},
					collection: map[string]*CollectionEntity{
						"item1": {IsSet: true, ItemID: "item1"},
						"item2": {IsSet: true, ItemID: "item2"},
					},
				},
			},
		},
	}

	for i := range tc.cache {
		tc.cache[i].onEvicted = append(tc.cache[i].onEvicted, func(itemID string, _ any) {
			tc.cache[i].offCollector.storeRemoveEntity(itemID, 1)
		})
	}

	exp := &TransCache{
		cache: map[string]*Cache{
			"testChID1": {
				lruIdx: list.New(),
				ttlIdx: list.New(),
				cache:  map[string]*cachedItem{},
				offCollector: &OfflineCollector{
					logger: &testLogger{log.New(&logBuf, "", 0)},
					collection: map[string]*CollectionEntity{
						"item1": {IsSet: false, ItemID: "item1"},
						"item2": {IsSet: false, ItemID: "item2"},
						"item3": {IsSet: false, ItemID: "item3"},
					},
				},
			},
			"testChID2": {
				lruIdx: list.New(),
				ttlIdx: list.New(),
				cache:  map[string]*cachedItem{},
				offCollector: &OfflineCollector{
					logger: &testLogger{log.New(&logBuf, "", 0)},
					collection: map[string]*CollectionEntity{
						"item1": {IsSet: false, ItemID: "item1"},
						"item2": {IsSet: false, ItemID: "item2"},
						"item3": {IsSet: false, ItemID: "item3"},
					},
				},
			},
		},
	}

	tc.Clear(nil)

	for key, value := range tc.cache {
		if len(value.cache) != len(exp.cache[key].cache) {
			t.Errorf("\nKey: <%+v>\nexpected nr of items: <%+v>, \nreceived nr of items: <%+v>",
				key, len(exp.cache[key].cache), len(value.cache))
		}
	}
	for key, c := range tc.cache {
		for collKey, coll := range c.offCollector.collection {
			if !reflect.DeepEqual(exp.cache[key].offCollector.collection[collKey], coll) {
				t.Errorf("Instance <%s>. Expected <%+v>, \nReceived <%+v>", key, exp.cache[key].offCollector.collection[collKey], coll)
			} else if rcv := logBuf.String(); rcv != "" {
				t.Errorf("Expected <%+v>, \nReceived <%+v>", "", rcv)
			}
		}
	}
}

func TestTranscacheGetItemExpiryTime(t *testing.T) {
	tc := &TransCache{
		cache: map[string]*Cache{
			"testChID": {
				cache: map[string]*cachedItem{
					"testItemID": {
						expiryTime: time.Time{},
					},
				},
			},
		},
	}
	chID := "testChID"
	itmID := "testItemID"

	var exp time.Time
	expok := true
	rcv, ok := tc.GetItemExpiryTime(chID, itmID)

	if ok != expok {
		t.Fatalf("\nexpected: <%+v>, \nreceived: <%+v>", expok, ok)
	}

	if rcv != exp {
		t.Errorf("\nexpected: <%+v>, \nreceived: <%+v>", exp, rcv)
	}
}

func TestTranscacheHasItem(t *testing.T) {
	tc := &TransCache{
		cache: map[string]*Cache{
			"testChID": {
				cache: map[string]*cachedItem{
					"testItemID": {},
				},
			},
		},
	}
	chID := "testChID"
	itmID := "testItemID"

	exp := true
	rcv := tc.HasItem(chID, itmID)

	if rcv != exp {
		t.Errorf("\nexpected: <%+v>, \nreceived: <%+v>", exp, rcv)
	}
}

func TestTranscacheNoItem(t *testing.T) {
	tc := &TransCache{
		cache: map[string]*Cache{
			"testChID": {
				cache: map[string]*cachedItem{
					"otherItem": {},
				},
			},
		},
	}
	chID := "testChID"
	itmID := "testItemID"

	exp := false
	rcv := tc.HasItem(chID, itmID)

	if rcv != exp {
		t.Errorf("\nexpected: <%+v>, \nreceived: <%+v>", exp, rcv)
	}
}

func TestTranscacheGetClonedNotFound(t *testing.T) {
	tc := &TransCache{
		cache: map[string]*Cache{
			"testChID": {
				cache: map[string]*cachedItem{
					"otherItem": {},
				},
			},
		},
	}
	chID := "testChID"
	itmID := "testItemID"

	experr := ErrNotFound
	rcv, err := tc.GetCloned(chID, itmID)

	if rcv != nil {
		t.Fatalf("\nexpected nil, \nreceived: <%+v>", rcv)
	}

	if err == nil || err != experr {
		t.Errorf("\nexpected: <%+v>, \nreceived: <%+v>", experr, err)
	}
}

func TestTranscacheGetClonedNotClonable(t *testing.T) {
	tc := &TransCache{
		cache: map[string]*Cache{
			"testChID": {
				lruIdx: list.New(),
				lruRefs: map[string]*list.Element{
					"testItemID": {},
				},
				cache: map[string]*cachedItem{
					"testItemID": {
						value: 3,
					},
				},
			},
		},
	}
	chID := "testChID"
	itmID := "testItemID"

	experr := ErrNotClonable
	rcv, err := tc.GetCloned(chID, itmID)

	if rcv != nil {
		t.Fatalf("\nexpected nil, \nreceived: <%+v>", rcv)
	}

	if err == nil || err != experr {
		t.Errorf("\nexpected: <%+v>, \nreceived: <%+v>", experr, err)
	}
}

type clonerMock struct {
	testcase string
}

func (cM *clonerMock) Clone() (interface{}, error) {
	switch cM.testcase {
	case "clone error":
		err := fmt.Errorf("clone mock error")
		return nil, err
	}
	return nil, nil
}
func TestTranscacheGetClonedCloneError(t *testing.T) {
	cloner := &clonerMock{
		testcase: "clone error",
	}
	tc := &TransCache{
		cache: map[string]*Cache{
			"testChID": {
				lruIdx: list.New(),
				lruRefs: map[string]*list.Element{
					"testItemID": {},
				},
				cache: map[string]*cachedItem{
					"testItemID": {
						value: cloner,
					},
				},
			},
		},
	}
	chID := "testChID"
	itmID := "testItemID"

	experr := "clone mock error"
	rcv, err := tc.GetCloned(chID, itmID)

	if rcv != nil {
		t.Fatalf("\nexpected nil, \nreceived: <%+v>", rcv)
	}

	if err == nil || err.Error() != experr {
		t.Errorf("\nexpected: <%+v>, \nreceived: <%+v>", experr, err)
	}
}

func TestTranscacheRewriteAllErr1(t *testing.T) {
	var logBuf bytes.Buffer
	tc := TransCache{
		cache: map[string]*Cache{
			"testChID1": {
				offCollector: &OfflineCollector{
					fldrPath: "/tmp/notexistent",
					logger:   &testLogger{log.New(&logBuf, "", 0)},
				},
			},
		},
		collectorParams: collectorParams{
			dumpInterval:    -1,
			rewriteInterval: -1,
		},
	}
	bufExpect := "error <lstat /tmp/notexistent: no such file or directory> walking path </tmp/notexistent>"
	tc.RewriteAll()
	if rcv := logBuf.String(); !strings.Contains(rcv, bufExpect) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", bufExpect, rcv)
	}
}

func TestTranscacheShutdownShutdownNil(t *testing.T) {
	tc := &TransCache{}
	exp := &TransCache{}
	tc.Shutdown()
	if !reflect.DeepEqual(exp, tc) {
		t.Errorf("Expected TransCache to not change, received <%+v>", tc)
	}
}

func TestTranscacheShutdownShutdownNoIntervalErr(t *testing.T) {
	path := "/tmp/*default"
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	f, err := os.OpenFile(path+"/file", os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644)
	if err != nil {
		t.Error(err)
	} else {
		defer f.Close()
	}
	f.Close()
	var logBuf bytes.Buffer
	tc := &TransCache{
		collectorParams: collectorParams{
			dumpInterval:    -1,
			rewriteInterval: -1,
		},
		cache: map[string]*Cache{
			DefaultCacheInstance: {
				offCollector: &OfflineCollector{
					logger: &testLogger{log.New(&logBuf, "", 0)},
					file:   f,
				},
			},
		},
	}
	expBuf := "error getting stats for file </tmp/*default/file>: stat /tmp/*default/file: file already closed"
	tc.Shutdown()
	if rcv := logBuf.String(); !strings.Contains(rcv, expBuf) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expBuf, rcv)
	}
}

func TestTranscacheShutdownShutdownRewriteErr(t *testing.T) {
	path := "/tmp/*default"
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	f, err := os.OpenFile(path+"/file", os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644)
	if err != nil {
		t.Error(err)
	} else {
		defer f.Close()
	}
	f.Close()
	var logBuf bytes.Buffer
	tc := &TransCache{
		collectorParams: collectorParams{
			dumpInterval:    -1,
			rewriteInterval: -2,
		},
		cache: map[string]*Cache{
			DefaultCacheInstance: {
				offCollector: &OfflineCollector{
					logger: &testLogger{log.New(&logBuf, "", 0)},
					file:   f,
				},
			},
		},
	}
	expBuf := "error getting stats for file </tmp/*default/file>: stat /tmp/*default/file: file already closed"
	tc.Shutdown()
	if rcv := logBuf.String(); !strings.Contains(rcv, expBuf) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expBuf, rcv)
	}
}

func TestTranscacheShutdownShutdownIntervalRewrite(t *testing.T) {
	path := "/tmp/*default"
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	f, err := os.OpenFile(path+"/file", os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644)
	if err != nil {
		t.Error(err)
	} else {
		defer f.Close()
	}
	f.Close()
	var logBuf bytes.Buffer
	tc := &TransCache{
		collectorParams: collectorParams{
			dumpInterval:    -1,
			rewriteInterval: 5 * time.Second,
			stopRewrite:     make(chan struct{}),
			rewriteStopped:  make(chan struct{}),
		},
		cache: map[string]*Cache{
			DefaultCacheInstance: {
				offCollector: &OfflineCollector{
					logger: &testLogger{log.New(&logBuf, "", 0)},
					file:   f,
				},
			},
		},
	}
	go func() {
		tc.Shutdown()
		expBuf := "error getting stats for file </tmp/*default/file>: stat /tmp/*default/file: file already closed"
		if rcv := logBuf.String(); !strings.Contains(rcv, expBuf) {
			t.Errorf("Expected <%+v>, \nReceived <%+v>", expBuf, rcv)
		}
	}()
	time.Sleep(50 * time.Millisecond)
	<-tc.collectorParams.stopRewrite
	tc.collectorParams.rewriteStopped <- struct{}{}
	time.Sleep(50 * time.Millisecond)
}

func TestTranscacheShutdownShutdownIntervalWrite(t *testing.T) {
	path := "/tmp/*default"
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	f, err := os.OpenFile(path+"/file", os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644)
	if err != nil {
		t.Error(err)
	} else {
		defer f.Close()
	}
	f.Close()
	var logBuf bytes.Buffer
	tc := &TransCache{
		collectorParams: collectorParams{
			dumpInterval:    5 * time.Second,
			rewriteInterval: -1,
			stopDump:        make(chan struct{}),
			dumpStopped:     make(chan struct{}),
		},
		cache: map[string]*Cache{
			DefaultCacheInstance: {
				offCollector: &OfflineCollector{
					logger: &testLogger{log.New(&logBuf, "", 0)},
					file:   f,
				},
			},
		},
	}
	go func() {
		tc.Shutdown()
		expBuf := "error getting stats for file </tmp/*default/file>: stat /tmp/*default/file: file already closed"
		if rcv := logBuf.String(); !strings.Contains(rcv, expBuf) {
			t.Errorf("Expected <%+v>, \nReceived <%+v>", expBuf, rcv)
		}
	}()
	time.Sleep(50 * time.Millisecond)
	<-tc.collectorParams.stopDump
	tc.collectorParams.dumpStopped <- struct{}{}
	time.Sleep(50 * time.Millisecond)
}

func TestTransCacheAsyncDumpEntities(t *testing.T) {
	path := "/tmp/internal_db"
	if err := os.MkdirAll(path+"/*default", 0755); err != nil {
		t.Fatal(err)
	}
	tmpFile, err := os.CreateTemp("/tmp/internal_db/*default", "testfile-*.txt")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	tc := &TransCache{
		cfg: map[string]*CacheConfig{
			DefaultCacheInstance: {},
		},
		cache: map[string]*Cache{
			DefaultCacheInstance: {
				offCollector: &OfflineCollector{
					file:             tmpFile,
					collectSetEntity: true,
					fldrPath:         "/tmp/internal_db/*default",
				},
			},
		},
		collectorParams: collectorParams{
			dumpInterval: 100 * time.Millisecond,
			dumpStopped:  make(chan struct{}),
			stopDump:     make(chan struct{}),
		},
	}
	tc.Set(DefaultCacheInstance, "CacheID", "sampleValue", []string{"CacheGroup1"}, true, "")
	go func() {
		tc.asyncDumpEntities()
		exp := &cachedItem{
			itemID:     "CacheID",
			value:      "sampleValue",
			expiryTime: time.Time{},
			groupIDs:   []string{"CacheGroup1"},
		}
		if !reflect.DeepEqual(exp, tc.cache[DefaultCacheInstance].cache["CacheID"]) {
			t.Errorf("Expected <%+v>, Received <%+v>", exp, tc.cache[DefaultCacheInstance].cache["CacheID"])
		}
	}()
	time.Sleep(150 * time.Millisecond)
	tc.collectorParams.stopDump <- struct{}{}
	time.Sleep(50 * time.Millisecond)
}

func TestCloseFileSuccess(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test-success-*.txt")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	fileName := tmpFile.Name()

	err = closeFile(tmpFile)

	if err != nil {
		t.Error(err)
	}

	if _, err := os.Stat(fileName); !os.IsNotExist(err) {
		t.Errorf("Expected file to be removed, but it still exists")
	}
}

func TestCloseFileErr(t *testing.T) {
	path := "/tmp/*default"
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	f, err := os.OpenFile(path+"/file", os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644)
	if err != nil {
		t.Error(err)
	} else {
		defer f.Close()
	}
	f.Close()
	expErr := "error getting stats for file </tmp/*default/file>: stat /tmp/*default/file: file already closed"
	if err := closeFile(f); err.Error() != expErr {
		t.Errorf("Expected error <%+v>, \nReceived error <%+v>", expErr, err)
	}
}

func TestNewTransCacheWithOfflineCollector(t *testing.T) {
	path := "/tmp/internal_db"
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	var logBuf bytes.Buffer
	tc, err := NewTransCacheWithOfflineCollector(path, 10*time.Second, 10*time.Second, -1, map[string]*CacheConfig{}, &testLogger{log.New(&logBuf, "", 0)})
	if err != nil {
		t.Error(err)
	} else if rcv := logBuf.String(); !strings.Contains(rcv, "") {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", "", rcv)
	}
	expTc := NewTransCache(map[string]*CacheConfig{})
	expTc.collectorParams = collectorParams{
		dumpInterval:    10 * time.Second,
		stopDump:        tc.collectorParams.stopDump,
		dumpStopped:     tc.collectorParams.dumpStopped,
		rewriteInterval: 10 * time.Second,
		stopRewrite:     tc.collectorParams.stopRewrite,
		rewriteStopped:  tc.collectorParams.rewriteStopped,
	}
	expTc.cache[DefaultCacheInstance].onEvicted = tc.cache[DefaultCacheInstance].onEvicted
	expTc.cache[DefaultCacheInstance].lruIdx = tc.cache[DefaultCacheInstance].lruIdx
	expTc.cache[DefaultCacheInstance].ttlIdx = tc.cache[DefaultCacheInstance].ttlIdx
	expTc.cache[DefaultCacheInstance].offCollector = &OfflineCollector{
		collection:       make(map[string]*CollectionEntity),
		fldrPath:         path + "/" + DefaultCacheInstance,
		collectSetEntity: true,
		writeLimit:       -1,
		file:             tc.cache[DefaultCacheInstance].offCollector.file,
		writer:           tc.cache[DefaultCacheInstance].offCollector.writer,
		encoder:          tc.cache[DefaultCacheInstance].offCollector.encoder,
		logger:           tc.cache[DefaultCacheInstance].offCollector.logger,
	}

	if !reflect.DeepEqual(expTc, tc) {
		t.Errorf("expected <%#v>, \nreceived <%#v>", expTc, tc)
	}
}

func TestNewTransCacheWithOfflineCollectorErr1(t *testing.T) {
	var logBuf bytes.Buffer
	_, err := NewTransCacheWithOfflineCollector("/tmp/doesntExist"+DefaultCacheInstance, 10*time.Second, 10*time.Second, -1, map[string]*CacheConfig{}, &testLogger{log.New(&logBuf, "", 0)})
	expErr := "stat /tmp/doesntExist*default: no such file or directory"
	if err == nil || expErr != err.Error() {
		t.Errorf("expected error <%v>, received <%v>", expErr, err)
	} else if rcv := logBuf.String(); !strings.Contains(rcv, "") {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", "", rcv)
	}

}

func TestNewTransCacheWithOfflineCollectorErr2(t *testing.T) {
	path := "/root"
	var logBuf bytes.Buffer
	_, err := NewTransCacheWithOfflineCollector(path, 10*time.Second, 10*time.Second, -1, map[string]*CacheConfig{}, &testLogger{log.New(&logBuf, "", 0)})
	expErr := "stat /root/*default: permission denied"
	if err == nil || expErr != err.Error() {
		t.Errorf("expected error <%v>, received <%v>", expErr, err)
	} else if rcv := logBuf.String(); !strings.Contains(rcv, "") {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", "", rcv)
	}

}

func TestNewTransCacheWithOfflineCollectorErr3(t *testing.T) {
	path := "/tmp/internal_db"
	if err := os.MkdirAll(path+"/*default", 0755); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path+"/*default/tmpfile", os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644)
	if err != nil {
		t.Error(err)
	}
	f.WriteString("somethin not decodable by gob")
	defer func() {
		f.Close()
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	var logBuf bytes.Buffer
	_, err = NewTransCacheWithOfflineCollector(path, 10*time.Second, 10*time.Second, -1, map[string]*CacheConfig{}, &testLogger{log.New(&logBuf, "", 0)})
	expErr := "failed to decode OfflineCacheEntity at </tmp/internal_db/*default/tmpfile>: unexpected EOF"
	if err == nil || expErr != err.Error() {
		t.Errorf("expected error <%v>, received <%v>", expErr, err)
	} else if rcv := logBuf.String(); !strings.Contains(rcv, "") {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", "", rcv)
	}
}

func TestTransCacheDumpAllDump0(t *testing.T) {
	tc := &TransCache{
		collectorParams: collectorParams{},
	}
	expTc := &TransCache{
		collectorParams: collectorParams{},
	}
	tc.DumpAll()
	if !reflect.DeepEqual(expTc, tc) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expTc, tc)
	}
	tc = &TransCache{
		collectorParams: collectorParams{
			dumpInterval: 0,
		},
	}
	expTc = &TransCache{
		collectorParams: collectorParams{
			dumpInterval: 0,
		},
	}
	tc.DumpAll()
	if !reflect.DeepEqual(expTc, tc) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expTc, tc)
	}
}

func TestTransCacheDumpAllDumpErr1(t *testing.T) {
	var logBuf bytes.Buffer
	tc := &TransCache{
		collectorParams: collectorParams{
			dumpInterval: 1 * time.Second,
		},
		cache: map[string]*Cache{
			DefaultCacheInstance: {
				cache: map[string]*cachedItem{
					"Item1": {
						itemID:   "Item1",
						value:    "val",
						groupIDs: []string{"gr1"},
					},
				},
				offCollector: &OfflineCollector{
					writeLimit: 1,
					collection: map[string]*CollectionEntity{
						"Item1": {
							IsSet:  true,
							ItemID: "Item1",
						},
					},
					logger: &testLogger{log.New(&logBuf, "", 0)},
				},
			},
		},
	}
	tc.DumpAll()
	expBuf := "error getting file stat: invalid argument"
	if rcv := logBuf.String(); !strings.Contains(rcv, expBuf) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expBuf, rcv)
	}
}

func TestTransCacheDumpAllDumpErr2(t *testing.T) {
	var logBuf bytes.Buffer
	tc := &TransCache{
		collectorParams: collectorParams{
			dumpInterval: 1 * time.Second,
		},
		cache: map[string]*Cache{
			DefaultCacheInstance: {
				cache: map[string]*cachedItem{
					"Item1": {
						itemID:   "Item1",
						value:    "val",
						groupIDs: []string{"gr1"},
					},
				},
				offCollector: &OfflineCollector{
					writeLimit: 1,
					collection: map[string]*CollectionEntity{
						"Item1": {
							IsSet:  false,
							ItemID: "Item1",
						},
					},
					logger: &testLogger{log.New(&logBuf, "", 0)},
				},
			},
		},
	}
	tc.DumpAll()
	expBuf := "error getting file stat: invalid argument"
	if rcv := logBuf.String(); !strings.Contains(rcv, expBuf) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expBuf, rcv)
	}
}

func TestTransCacheRewriteAllDump0(t *testing.T) {
	tc := &TransCache{
		collectorParams: collectorParams{},
	}
	expTc := &TransCache{
		collectorParams: collectorParams{},
	}
	tc.RewriteAll()
	if !reflect.DeepEqual(expTc, tc) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expTc, tc)
	}
	tc = &TransCache{
		collectorParams: collectorParams{
			dumpInterval: 0,
		},
	}
	expTc = &TransCache{
		collectorParams: collectorParams{
			dumpInterval: 0,
		},
	}
	tc.RewriteAll()
	if !reflect.DeepEqual(expTc, tc) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expTc, tc)
	}
	tc = &TransCache{
		collectorParams: collectorParams{
			rewriteInterval: 0,
		},
	}
	expTc = &TransCache{
		collectorParams: collectorParams{
			rewriteInterval: 0,
		},
	}
	tc.RewriteAll()
	if !reflect.DeepEqual(expTc, tc) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expTc, tc)
	}
}

func TestTransCacheAsyncRewriteEntitiesMinus1NoChanges(t *testing.T) {
	path := "/tmp/internal_db"
	if err := os.MkdirAll(path+"/*default", 0755); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	file, err := os.OpenFile(path+"/*default/file1", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Error(err)
	}
	writer := bufio.NewWriter(file)
	encoder := gob.NewEncoder(writer)
	if err := encodeAndDump(OfflineCacheEntity{IsSet: true,
		ItemID: "item1", Value: "val1", GroupIDs: []string{"gr1"}}, encoder, writer); err != nil {
		t.Error(err)
	}
	file.Close()
	file, err = os.OpenFile(path+"/*default/file2", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Error(err)
	}
	writer = bufio.NewWriter(file)
	encoder = gob.NewEncoder(writer)
	if err := encodeAndDump(OfflineCacheEntity{IsSet: true,
		ItemID: "item2", Value: "val2", GroupIDs: []string{"gr2"}}, encoder, writer); err != nil {
		t.Error(err)
	}
	file.Close()
	if files, err := os.ReadDir(path + "/*default"); err != nil {
		t.Error(err)
	} else if len(files) != 2 {
		t.Errorf("expected 2 files in <%v>, received <%v>", path+"/*default", len(files))
	}
	var logBuf bytes.Buffer
	c, err := newCacheFromFolder(path+"/*default", -1, 0, false, &testLogger{log.New(&logBuf, "", 0)},
		20, -1, nil)
	if err != nil {
		t.Error(err)
	}
	tc := &TransCache{
		collectorParams: collectorParams{
			dumpInterval:    10000 * time.Millisecond,
			rewriteInterval: -1,
		},
		cache: map[string]*Cache{
			DefaultCacheInstance: c,
		},
	}
	tc.asyncRewriteEntities()
	time.Sleep(100 * time.Millisecond)
	expBuf := ""
	if rcv := logBuf.String(); !strings.Contains(rcv, expBuf) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expBuf, rcv)
	}

	if _, err := os.ReadDir(path + "/*default"); err != nil {
		t.Error(err)
	}

	f, err := os.Open(path + "/*default/0Rewrite0")
	if err != nil {
		t.Error(err)
	}
	dc := gob.NewDecoder(f)
	var rcv *OfflineCacheEntity
	if err := dc.Decode(&rcv); err != nil {
		t.Error(err)
	}
	exp := []*OfflineCacheEntity{
		{
			IsSet:    true,
			ItemID:   "item1",
			Value:    "val1",
			GroupIDs: []string{"gr1"},
		},
		{
			IsSet:    true,
			ItemID:   "item2",
			Value:    "val2",
			GroupIDs: []string{"gr2"},
		},
	}
	if !reflect.DeepEqual(exp[0], rcv) {
		if !reflect.DeepEqual(exp[1], rcv) {
			t.Errorf("expected <%+v>, received <%+v>", exp, rcv)
		}
	}
	rcv = nil
	if err := dc.Decode(&rcv); err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(exp[0], rcv) {
		if !reflect.DeepEqual(exp[1], rcv) {
			t.Errorf("expected <%+v>, received <%+v>", exp, rcv)
		}
	}

}

func TestTransCacheAsyncRewriteEntitiesMinus1Changes(t *testing.T) {
	path := "/tmp/internal_db"
	if err := os.MkdirAll(path+"/*default", 0755); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	file, err := os.OpenFile(path+"/*default/file1", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Error(err)
	}
	writer := bufio.NewWriter(file)
	encoder := gob.NewEncoder(writer)
	if err := encodeAndDump(OfflineCacheEntity{IsSet: true,
		ItemID: "item1", Value: "val1", GroupIDs: []string{"gr1"}}, encoder, writer); err != nil {
		t.Error(err)
	}
	file.Close()
	file, err = os.OpenFile(path+"/*default/file2", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Error(err)
	}
	writer = bufio.NewWriter(file)
	encoder = gob.NewEncoder(writer)
	if err := encodeAndDump(OfflineCacheEntity{IsSet: false,
		ItemID: "item1"}, encoder, writer); err != nil {
		t.Error(err)
	}
	file.Close()

	if files, err := os.ReadDir(path + "/*default"); err != nil {
		t.Error(err)
	} else if len(files) != 2 {
		t.Errorf("expected 2 files in <%v>, received <%v>", path+"/*default", len(files))
	}

	var logBuf bytes.Buffer
	c, err := newCacheFromFolder(path+"/*default", -1, 0, false, &testLogger{log.New(&logBuf, "", 0)},
		-1, -1, nil)
	if err != nil {
		t.Error(err)
	}
	tc := &TransCache{
		collectorParams: collectorParams{
			dumpInterval:    10000 * time.Millisecond,
			rewriteInterval: -1,
		},
		cache: map[string]*Cache{
			DefaultCacheInstance: c,
		},
	}
	tc.asyncRewriteEntities()
	time.Sleep(100 * time.Millisecond)
	expBuf := ""
	if rcv := logBuf.String(); !strings.Contains(rcv, expBuf) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expBuf, rcv)
	}

	if _, err := os.ReadDir(path + "/*default"); err != nil {
		t.Error(err)
	}

	var builder strings.Builder
	files, err := os.ReadDir(path + "/*default")
	if err != nil {
		t.Fatalf("Error reading directory: %v", err)
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		filePath := filepath.Join(path+"/*default", file.Name())
		content, err := os.ReadFile(filePath)
		if err != nil {
			t.Fatalf("Error reading file %s: %v", file.Name(), err)
		}

		builder.Write(content)
	}
	combinedContent := builder.String()

	if combinedContent != "" {
		t.Errorf("Expected empty file, received <%s>", combinedContent)
	}
}

func TestTransCacheAsyncRewriteEntitiesIntervalChanges(t *testing.T) {
	path := "/tmp/internal_db"
	if err := os.MkdirAll(path+"/*default", 0755); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	file, err := os.OpenFile(path+"/*default/file1", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Error(err)
	}
	writer := bufio.NewWriter(file)
	encoder := gob.NewEncoder(writer)
	if err := encodeAndDump(OfflineCacheEntity{IsSet: true,
		ItemID: "item1", Value: "val1", GroupIDs: []string{"gr1"}}, encoder, writer); err != nil {
		t.Error(err)
	}
	file.Close()
	file, err = os.OpenFile(path+"/*default/file2", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Error(err)
	}
	writer = bufio.NewWriter(file)
	encoder = gob.NewEncoder(writer)
	if err := encodeAndDump(OfflineCacheEntity{IsSet: false,
		ItemID: "item1"}, encoder, writer); err != nil {
		t.Error(err)
	}
	file.Close()

	if files, err := os.ReadDir(path + "/*default"); err != nil {
		t.Error(err)
	} else if len(files) != 2 {
		t.Errorf("expected 2 files in <%v>, received <%v>", path+"/*default", len(files))
	}

	var logBuf bytes.Buffer
	c, err := newCacheFromFolder(path+"/*default", -1, 0, false, &testLogger{log.New(&logBuf, "", 0)},
		-1, -1, nil)
	if err != nil {
		t.Error(err)
	}
	tc := &TransCache{
		collectorParams: collectorParams{
			dumpInterval:    1000 * time.Millisecond,
			rewriteInterval: 10 * time.Millisecond,
			stopRewrite:     make(chan struct{}),
			rewriteStopped:  make(chan struct{}),
		},
		cache: map[string]*Cache{
			DefaultCacheInstance: c,
		},
	}

	go func() {
		tc.asyncRewriteEntities()
		expBuf := ""
		if rcv := logBuf.String(); !strings.Contains(rcv, expBuf) {
			t.Errorf("Expected <%+v>, \nReceived <%+v>", expBuf, rcv)
		}

		if _, err := os.ReadDir(path + "/*default"); err != nil {
			t.Error(err)
		}

		var builder strings.Builder
		files, err := os.ReadDir(path + "/*default")
		if err != nil {
			t.Fatalf("Error reading directory: %v", err)
		}

		for _, file := range files {
			if file.IsDir() {
				continue
			}

			filePath := filepath.Join(path+"/*default", file.Name())
			content, err := os.ReadFile(filePath)
			if err != nil {
				t.Fatalf("Error reading file %s: %v", file.Name(), err)
			}

			builder.Write(content)
		}
		combinedContent := builder.String()

		if combinedContent != "" {
			t.Errorf("Expected empty file, received <%s>", combinedContent)
		}
	}()
	time.Sleep(150 * time.Millisecond)
	tc.collectorParams.stopRewrite <- struct{}{}
	time.Sleep(50 * time.Millisecond)

}

// BenchmarkSet            	 3000000	       469 ns/op
func BenchmarkSet(b *testing.B) {
	cacheItems := [][]string{
		{"aaa_", "1", "1"},
		{"aaa_", "2", "1"},
		{"aaa_", "3", "1"},
		{"aaa_", "4", "1"},
		{"aaa_", "5", "1"},
	}
	rand.Seed(time.Now().UTC().UnixNano())
	min, max := 0, len(cacheItems)-1 // so we can have random index
	tc := NewTransCache(map[string]*CacheConfig{})
	for n := 0; n < b.N; n++ {
		ci := cacheItems[rand.Intn(max-min)+min]
		tc.Set(ci[0], ci[1], ci[2], nil, false, "")
	}
}

// BenchmarkSetWithGroups  	 3000000	       591 ns/op
func BenchmarkSetWithGroups(b *testing.B) {
	cacheItems := [][]string{
		{"aaa_", "1", "1"},
		{"aaa_", "2", "1"},
		{"aaa_", "3", "1"},
		{"aaa_", "4", "1"},
		{"aaa_", "5", "1"},
	}
	rand.Seed(time.Now().UTC().UnixNano())
	min, max := 0, len(cacheItems)-1 // so we can have random index
	tc := NewTransCache(map[string]*CacheConfig{})
	for n := 0; n < b.N; n++ {
		ci := cacheItems[rand.Intn(max-min)+min]
		tc.Set(ci[0], ci[1], ci[2], []string{"grp1", "grp2"}, false, "")
	}
}

// BenchmarkGet            	10000000	       163 ns/op
func BenchmarkGet(b *testing.B) {
	cacheItems := [][]string{
		{"aaa_", "1", "1"},
		{"aaa_", "2", "1"},
		{"aaa_", "3", "1"},
		{"aaa_", "4", "1"},
		{"aaa_", "5", "1"},
	}
	tc := NewTransCache(map[string]*CacheConfig{})
	for _, ci := range cacheItems {
		tc.Set(ci[0], ci[1], ci[2], nil, false, "")
	}
	rand.Seed(time.Now().UTC().UnixNano())
	min, max := 0, len(cacheItems)-1 // so we can have random index
	for n := 0; n < b.N; n++ {
		tc.Get("aaa_", cacheItems[rand.Intn(max-min)+min][0])
	}
}
