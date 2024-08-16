/*
TransCache is released under the MIT License <http://www.opensource.org/licenses/mit-license.php
Copyright (C) ITsysCOM GmbH. All Rights Reserved.

TransCache is a bigger version of Cache with support for multiple Cache instances and transactions
*/

package ltcache

import (
	"bytes"
	"crypto/rand"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"time"
)

const (
	AddItem              = "AddItem"
	RemoveItem           = "RemoveItem"
	RemoveGroup          = "RemoveGroup"
	DefaultCacheInstance = "*default"
)

var (
	ErrNotFound    = errors.New("not found")
	ErrNotClonable = errors.New("not clonable")
)

func GenUUID() string {
	b := make([]byte, 16)
	io.ReadFull(rand.Reader, b)
	b[6] = (b[6] & 0x0F) | 0x40
	b[8] = (b[8] &^ 0x40) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10],
		b[10:])
}

// Cloner is an interface for objects to clone themselves into interface
type Cloner interface {
	Clone() (interface{}, error)
}

type transactionItem struct {
	verb     string      // action which will be executed on cache
	cacheID  string      // cache instance identifier
	itemID   string      // item itentifier
	value    interface{} // item value
	groupIDs []string    // attach item to groups
}

type DumpTransactionItem struct {
	Verb     string      // action which will be executed on cache
	CacheID  string      // cache instance identifier
	ItemID   string      // item itentifier
	Value    interface{} // item value
	GroupIDs []string    // attach item to groups
}

type CacheConfig struct {
	MaxItems  int
	TTL       time.Duration
	StaticTTL bool
	OnEvicted func(itmID string, value interface{})
}

// NewTransCache instantiates a new TransCache
func NewTransCache(cfg map[string]*CacheConfig) (tc *TransCache) {
	if _, has := cfg[DefaultCacheInstance]; !has { // Default always created
		cfg[DefaultCacheInstance] = &CacheConfig{MaxItems: -1}
	}
	tc = &TransCache{
		cache:             make(map[string]*Cache),
		cfg:               cfg,
		transactionBuffer: make(map[string][]*transactionItem),
	}
	for cacheID, chCfg := range cfg {
		tc.cache[cacheID] = NewCache(chCfg.MaxItems, chCfg.TTL, chCfg.StaticTTL, chCfg.OnEvicted)
	}
	return
}

// TransCache is a bigger cache with transactions and multiple Cache instances support
type TransCache struct {
	cache    map[string]*Cache       // map[cacheInstance]cacheStore
	cfg      map[string]*CacheConfig // map[cacheInstance]*CacheConfig
	cacheMux sync.RWMutex            // so we can apply the complete transaction buffer in one shoot

	transactionBuffer map[string][]*transactionItem // Queue tasks based on transactionID
	transBufMux       sync.Mutex                    // Protects the transactionBuffer
	transactionMux    sync.Mutex                    // Queue transactions on commit
}

// Hold copy of original TransCache with necessary exportable fields
type DumpTransCache struct {
	Cache             map[string]*DumpCache
	TransactionBuffer map[string][]*DumpTransactionItem
}

func (tItem *transactionItem) toDumpTransactionItem() *DumpTransactionItem {
	return &DumpTransactionItem{
		Verb:     tItem.verb,
		CacheID:  tItem.cacheID,
		ItemID:   tItem.itemID,
		Value:    tItem.value,
		GroupIDs: tItem.groupIDs,
	}
}

func newTransBufferFromDump(dmp *DumpTransactionItem) *transactionItem {
	return &transactionItem{
		verb:     dmp.Verb,
		cacheID:  dmp.CacheID,
		itemID:   dmp.ItemID,
		value:    dmp.Value,
		groupIDs: dmp.GroupIDs,
	}
}

// Method to populate DumpTransCache struct with values of original TransCache
func (tc *TransCache) toDumpTransCache() *DumpTransCache {
	dmpTC := &DumpTransCache{
		Cache:             make(map[string]*DumpCache, len(tc.cache)),
		TransactionBuffer: make(map[string][]*DumpTransactionItem, len(tc.transactionBuffer)),
	}
	tc.cacheMux.RLock()
	defer tc.cacheMux.RUnlock()
	for key, cache := range tc.cache {
		dmpCache := cache.toDumpCache()
		dmpTC.Cache[key] = &DumpCache{
			Cache:  dmpCache.Cache,
			Groups: dmpCache.Groups,
		}
	}
	for transID, transItems := range tc.transactionBuffer {
		var dmpTransBuff []*DumpTransactionItem
		for _, transItem := range transItems {
			dmpTransBuff = append(dmpTransBuff, transItem.toDumpTransactionItem())
		}
		dmpTC.TransactionBuffer[transID] = dmpTransBuff
	}
	return dmpTC
}

// Method to populate TransCache with values of recovered DumpTransCache
func newTransCacheFromDump(dmpTC *DumpTransCache, cfg map[string]*CacheConfig) *TransCache {
	if _, has := cfg[DefaultCacheInstance]; !has { // Default always created
		cfg[DefaultCacheInstance] = &CacheConfig{MaxItems: -1}
	}
	tc := &TransCache{
		cache:             make(map[string]*Cache, len(dmpTC.Cache)),
		cfg:               cfg,
		transactionBuffer: make(map[string][]*transactionItem, len(dmpTC.TransactionBuffer)),
	}

	for key, dmpCache := range dmpTC.Cache {
		tc.cache[key] = newCacheFromDump(dmpCache, cfg[key].MaxItems, cfg[key].TTL, cfg[key].StaticTTL, cfg[key].OnEvicted)
	}
	for dumpTransID, dumpTransItem := range dmpTC.TransactionBuffer {
		var transBuff []*transactionItem
		for _, transItem := range dumpTransItem {
			transBuff = append(transBuff, newTransBufferFromDump(transItem))
		}
		tc.transactionBuffer[dumpTransID] = transBuff
	}
	return tc
}

// Will read all data from dump file and put them back on a new TransCache
func ReadAll(decoder *gob.Decoder, cfg map[string]*CacheConfig) (tc *TransCache, err error) {

	var dmpTC *DumpTransCache
	err = decoder.Decode(&dmpTC)
	if err != nil {
		return nil, err
	}

	tc = newTransCacheFromDump(dmpTC, cfg)

	// Debug just for checking if fields populate properly
	// fmt.Println("newTc.transactionBuffer", tc.transactionBuffer)
	// for cacheinstance, cache := range tc.cache {
	// 	fmt.Println("cacheinstance", cacheinstance)
	// 	fmt.Println("cache.groups", cache.groups)
	// 	fmt.Println("cache.lruIdx", cache.lruIdx.Front())
	// 	fmt.Println("cache.lruRefs", cache.lruRefs)
	// 	fmt.Println("cache.ttlIdx", cache.ttlIdx.Front())
	// 	fmt.Println("cache.ttlRefs", cache.ttlRefs)
	// 	for chID, cachedItem := range cache.cache {
	// 		fmt.Println("chID", chID)
	// 		fmt.Println("cachedItem.itemID", cachedItem.itemID)
	// 		fmt.Println("cachedItem.value", cachedItem.value)
	// 		fmt.Println("cachedItem.expiryTime", cachedItem.expiryTime)
	// 		fmt.Println("cachedItem.groupIDs", cachedItem.groupIDs)
	// 	}
	// }
	return tc, nil
}

// WriteAll will write all of TransCache in a dump file
func (tc *TransCache) WriteAll(w io.Writer, encoder *gob.Encoder, buf *bytes.Buffer) error {

	// file, err := os.Create(filename)
	// if err != nil {
	// 	return fmt.Errorf("failed to create file: %w", err)
	// }
	// defer file.Close()
	staratWrite := time.Now()
	tc.cacheMux.RLock()
	tc.transBufMux.Lock()
	tc.transactionMux.Lock()
	defer func() {
		tc.cacheMux.RUnlock()
		tc.transBufMux.Unlock()
		tc.transactionMux.Unlock()
		writeTime := time.Since(staratWrite)
		fmt.Println("writeTime", writeTime)
	}()

	// fmt.Println("tc.transactionBuffer", tc.transactionBuffer)
	// for cacheinstance, cache := range tc.cache {
	// 	fmt.Println("cacheinstance", cacheinstance)
	// 	fmt.Println("cache.groups", cache.groups)
	// 	fmt.Println("cache.lruIdx", cache.lruIdx)
	// 	fmt.Println("cache.lruRefs", cache.lruRefs)
	// 	fmt.Println("cache.ttlIdx", cache.ttlIdx)
	// 	fmt.Println("cache.ttlRefs", cache.ttlRefs)
	// 	for chID, cachedItem := range cache.cache {
	// 		fmt.Println("chID", chID)
	// 		fmt.Println("cachedItem.itemID", cachedItem.itemID)
	// 		fmt.Println("cachedItem.value", cachedItem.value)
	// 		fmt.Println("cachedItem.expiryTime", cachedItem.expiryTime)
	// 		fmt.Println("cachedItem.groupIDs", cachedItem.groupIDs)
	// 	}
	// }

	// var buf bytes.Buffer
	// encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(*tc.toDumpTransCache())
	if err != nil {
		fmt.Println("Error encoding data:", err)
		return err
	}
	_, err = buf.WriteTo(w)
	if err != nil {
		fmt.Println("Error writing to file:", err)
		return err
	}
	return nil
}

// cacheInstance returns a specific cache instance based on ID or default
func (tc *TransCache) cacheInstance(chID string) (c *Cache) {
	var ok bool
	if c, ok = tc.cache[chID]; !ok {
		c = tc.cache[DefaultCacheInstance]
	}
	return
}

// BeginTransaction initializes a new transaction into transactions buffer
func (tc *TransCache) BeginTransaction() (transID string) {
	transID = GenUUID()
	tc.transBufMux.Lock()
	tc.transactionBuffer[transID] = make([]*transactionItem, 0)
	tc.transBufMux.Unlock()
	return transID
}

// RollbackTransaction destroys a transaction from transactions buffer
func (tc *TransCache) RollbackTransaction(transID string) {
	tc.transBufMux.Lock()
	delete(tc.transactionBuffer, transID)
	tc.transBufMux.Unlock()
}

// CommitTransaction executes the actions in a transaction buffer
func (tc *TransCache) CommitTransaction(transID string) {
	tc.transactionMux.Lock()
	tc.transBufMux.Lock()
	tc.cacheMux.Lock() // apply all transactioned items in one shot
	for _, item := range tc.transactionBuffer[transID] {
		switch item.verb {
		case AddItem:
			tc.Set(item.cacheID, item.itemID, item.value, item.groupIDs, true, transID)
		case RemoveItem:
			tc.Remove(item.cacheID, item.itemID, true, transID)
		case RemoveGroup:
			if len(item.groupIDs) >= 1 {
				tc.RemoveGroup(item.cacheID, item.groupIDs[0], true, transID)
			}
		}
	}
	tc.cacheMux.Unlock()
	delete(tc.transactionBuffer, transID)
	tc.transBufMux.Unlock()
	tc.transactionMux.Unlock()
}

// Get returns the value of an Item
func (tc *TransCache) Get(chID, itmID string) (interface{}, bool) {
	tc.cacheMux.RLock()
	defer tc.cacheMux.RUnlock()
	return tc.cacheInstance(chID).Get(itmID)
}

// Set will add/edit an item to the cache
func (tc *TransCache) Set(chID, itmID string, value interface{},
	groupIDs []string, commit bool, transID string) {
	if commit {
		if transID == "" { // Lock locally
			tc.cacheMux.Lock()
			defer tc.cacheMux.Unlock()
		}
		tc.cacheInstance(chID).Set(itmID, value, groupIDs)
	} else {
		tc.transBufMux.Lock()
		tc.transactionBuffer[transID] = append(tc.transactionBuffer[transID],
			&transactionItem{cacheID: chID,
				verb: AddItem, itemID: itmID,
				value: value, groupIDs: groupIDs})
		tc.transBufMux.Unlock()
	}
}

// RempveItem removes an item from the cache
func (tc *TransCache) Remove(chID, itmID string, commit bool, transID string) {
	if commit {
		if transID == "" { // Lock per operation not transaction
			tc.cacheMux.Lock()
			defer tc.cacheMux.Unlock()
		}
		tc.cacheInstance(chID).Remove(itmID)
	} else {
		tc.transBufMux.Lock()
		tc.transactionBuffer[transID] = append(tc.transactionBuffer[transID],
			&transactionItem{cacheID: chID, verb: RemoveItem, itemID: itmID})
		tc.transBufMux.Unlock()
	}
}

func (tc *TransCache) HasGroup(chID, grpID string) (has bool) {
	tc.cacheMux.RLock()
	has = tc.cacheInstance(chID).HasGroup(grpID)
	tc.cacheMux.RUnlock()
	return
}

// GetGroupItems returns all items in a group. Nil if group does not exist
func (tc *TransCache) GetGroupItemIDs(chID, grpID string) (itmIDs []string) {
	tc.cacheMux.RLock()
	itmIDs = tc.cacheInstance(chID).GetGroupItemIDs(grpID)
	tc.cacheMux.RUnlock()
	return
}

// GetGroupItems returns all items in a group. Nil if group does not exist
func (tc *TransCache) GetGroupItems(chID, grpID string) (itms []interface{}) {
	tc.cacheMux.RLock()
	itms = tc.cacheInstance(chID).GetGroupItems(grpID)
	tc.cacheMux.RUnlock()
	return
}

// RemoveGroup removes a group of items out of cache
func (tc *TransCache) RemoveGroup(chID, grpID string, commit bool, transID string) {
	if commit {
		if transID == "" { // Lock locally
			tc.cacheMux.Lock()
			defer tc.cacheMux.Unlock()
		}
		tc.cacheInstance(chID).RemoveGroup(grpID)
	} else {
		tc.transBufMux.Lock()
		tc.transactionBuffer[transID] = append(tc.transactionBuffer[transID],
			&transactionItem{cacheID: chID, verb: RemoveGroup, groupIDs: []string{grpID}})
		tc.transBufMux.Unlock()
	}
}

// Remove all items in one or more cache instances
func (tc *TransCache) Clear(chIDs []string) {
	tc.cacheMux.Lock()
	if chIDs == nil {
		chIDs = make([]string, len(tc.cache))
		i := 0
		for chID := range tc.cache {
			chIDs[i] = chID
			i += 1
		}
	}
	for _, chID := range chIDs {
		tc.cacheInstance(chID).Clear()
	}
	tc.cacheMux.Unlock()
}

// GetCloned returns a clone of an Item if Item is clonable
func (tc *TransCache) GetCloned(chID, itmID string) (cln interface{}, err error) {
	tc.cacheMux.RLock()
	origVal, hasIt := tc.cacheInstance(chID).Get(itmID)
	tc.cacheMux.RUnlock()
	if !hasIt {
		return nil, ErrNotFound
	}
	if origVal == nil {
		return
	}
	if _, canClone := origVal.(Cloner); !canClone {
		return nil, ErrNotClonable
	}
	retVals := reflect.ValueOf(origVal).MethodByName("Clone").Call(nil) // Call Clone method on the object
	errIf := retVals[1].Interface()
	var notNil bool
	if err, notNil = errIf.(error); notNil {
		return
	}
	return retVals[0].Interface(), nil
}

// GetItemIDs returns a list of item IDs matching prefix
func (tc *TransCache) GetItemIDs(chID, prfx string) (itmIDs []string) {
	tc.cacheMux.RLock()
	itmIDs = tc.cacheInstance(chID).GetItemIDs(prfx)
	tc.cacheMux.RUnlock()
	return
}

// GetItemExpiryTime returns the expiry time of an item, ok is false if not found
func (tc *TransCache) GetItemExpiryTime(chID, itmID string) (exp time.Time, ok bool) {
	tc.cacheMux.RLock()
	defer tc.cacheMux.RUnlock()
	return tc.cacheInstance(chID).GetItemExpiryTime(itmID)
}

// HasItem verifies if Item is in the cache
func (tc *TransCache) HasItem(chID, itmID string) (has bool) {
	tc.cacheMux.RLock()
	has = tc.cacheInstance(chID).HasItem(itmID)
	tc.cacheMux.RUnlock()
	return
}

// GetCacheStats returns on overview of full cache
func (tc *TransCache) GetCacheStats(chIDs []string) (cs map[string]*CacheStats) {
	cs = make(map[string]*CacheStats)
	tc.cacheMux.RLock()
	if len(chIDs) == 0 {
		for chID := range tc.cache {
			chIDs = append(chIDs, chID)
		}
	}
	for _, chID := range chIDs {
		cs[chID] = tc.cacheInstance(chID).GetCacheStats()
	}
	tc.cacheMux.RUnlock()
	return
}
