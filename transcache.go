/*
TransCache is released under the MIT License <http://www.opensource.org/licenses/mit-license.php
Copyright (C) ITsysCOM GmbH. All Rights Reserved.

TransCache is a bigger version of Cache with support for multiple Cache instances and transactions
*/

package ltcache

import (
	"bufio"
	"crypto/rand"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"reflect"
	"slices"
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

	offCollector *OfflineCollector // used to temporarily hold caching instances, until dumped to file
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
		c := tc.cacheInstance(chID)
		c.Set(itmID, value, groupIDs)
		if tc.offCollector != nil {
			c.RLock()
			if err := tc.storeCache(chID, itmID); err != nil {
				tc.offCollector.logger.Err(err.Error())
			}
			c.RUnlock()
		}
	} else {
		tc.transBufMux.Lock()
		tc.transactionBuffer[transID] = append(tc.transactionBuffer[transID],
			&transactionItem{cacheID: chID,
				verb: AddItem, itemID: itmID,
				value: value, groupIDs: groupIDs})
		tc.transBufMux.Unlock()
	}
}

// Decides weather to write the cache on file instantly or put it in the collector to store in intervals
func (tc *TransCache) storeCache(chInstance, cacheID string) (err error) {
	if tc.offCollector.dumpInterval == 0 {
		return
	}
	if tc.offCollector.dumpInterval == -1 {
		tc.offCollector.setCollMux[chInstance].Lock()
		defer tc.offCollector.setCollMux[chInstance].Unlock()
		return tc.offCollector.writeSetEntity(chInstance, cacheID,
			tc.cache[chInstance].cache[cacheID].value,
			tc.cache[chInstance].cache[cacheID].expiryTime,
			tc.cache[chInstance].cache[cacheID].groupIDs)
	}
	tc.offCollector.collect(chInstance, cacheID)
	return
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
		if tc.offCollector != nil {
			tc.offCollector.clearOfflineInstance(chID)
		}
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

// NewTransCache instantiates a new TransCache with constructed OfflineCollector
func NewTransCacheWithOfflineCollector(fldrPath string, dumpInterval, rewriteInterval time.Duration, writeLimit int, cfg map[string]*CacheConfig, l logger) (tc *TransCache, err error) {
	if err := ensureDir(fldrPath); err != nil {
		return nil, err
	}
	if _, exists := cfg[DefaultCacheInstance]; !exists {
		cfg[DefaultCacheInstance] = &CacheConfig{MaxItems: -1}
	}
	tc = &TransCache{
		cache:             make(map[string]*Cache),
		cfg:               cfg,
		transactionBuffer: make(map[string][]*transactionItem),
		offCollector: &OfflineCollector{
			setCollMux:      make(map[string]*sync.RWMutex),
			files:           make(map[string]*os.File),
			writers:         make(map[string]*bufio.Writer),
			encoders:        make(map[string]*gob.Encoder),
			writeLimit:      writeLimit,
			setColl:         make(map[string]map[string]*OfflineCacheEntity),
			remColl:         make(map[string][]string),
			folderPath:      fldrPath,
			dumpInterval:    dumpInterval,
			rewriteInterval: rewriteInterval,
			logger:          l,
			stopWriting:     make(chan struct{}),
			writeStopped:    make(chan struct{}),
			stopRewrite:     make(chan struct{}),
			rewriteStopped:  make(chan struct{}),
		},
	}
	err = tc.readAll()
	return
}

// Reads from dump files and starts dynamicaly backing up the cache
func (tc *TransCache) readAll() error {

	var wg sync.WaitGroup
	errChan := make(chan error, 1)
	done := make(chan struct{})
	var tcCacheMux sync.RWMutex
	for chInstance, config := range tc.cfg {
		if err := ensureDir(path.Join(tc.offCollector.folderPath, chInstance)); err != nil {
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := processDumpFiles(chInstance, tc.offCollector.folderPath, config.MaxItems, config.TTL, config.StaticTTL, tc, &tcCacheMux); err != nil {
				errChan <- err
				return
			}
		}()
		if err := tc.offCollector.populateEncoders(chInstance); err != nil {
			return err
		}
		tc.offCollector.setCollMux[chInstance] = new(sync.RWMutex)
	}

	go func() {
		wg.Wait()
		if tc.offCollector.rewriteInterval == -1 {
			tc.Rewrite()
		}
		close(done)
	}()

	select {
	case err := <-errChan:
		return err
	case <-done:
		if tc.offCollector.rewriteInterval > 0 {
			go tc.offCollector.runRewrite()
		}
		if tc.offCollector.dumpInterval == -1 {
			return nil
		}
		go tc.asyncWriteEntities()
		return nil
	}
}

// Write the OfflineCollection cache items on file every dumpInterval
func (tc *TransCache) asyncWriteEntities() {
	if tc.offCollector.dumpInterval <= 0 {
		close(tc.offCollector.writeStopped)
		return
	}
	for {
		select {
		case <-tc.offCollector.stopWriting: // in case engine is shutdown before interval, dont wait for it
			close(tc.offCollector.writeStopped)
			return
		case <-time.After(tc.offCollector.dumpInterval): // no need to instantly write right after reading from files
			if err := tc.WriteAll(); err != nil {
				tc.offCollector.logger.Err(err.Error())
			}
		}
	}
}

// Dumps all of collected cache in files
func (tc *TransCache) WriteAll() error {
	if tc.offCollector == nil {
		return fmt.Errorf("InternalDB dump not activated")
	}
	var wg sync.WaitGroup
	errChan := make(chan error, 1) // used to stop and return the function if there are errors
	done := make(chan struct{}, 1) // used to signal when all writing is finished
	var chInstanceList []string    // will hold coply cache Instance list to avoid concurrency
	tc.offCollector.allCollMux.RLock()
	for chI := range tc.offCollector.setColl {
		tc.offCollector.setCollMux[chI].RLock()
		if len(tc.offCollector.setColl[chI]) != 0 {
			chInstanceList = append(chInstanceList, chI)
		}
		tc.offCollector.setCollMux[chI].RUnlock()
	}
	tc.offCollector.allCollMux.RUnlock()
	tc.offCollector.remCollMux.RLock()
	for chI := range tc.offCollector.remColl {
		if !slices.Contains(chInstanceList, chI) {
			if len(tc.offCollector.remColl[chI]) != 0 {
				chInstanceList = append(chInstanceList, chI)
			}
		}
	}
	tc.offCollector.remCollMux.RUnlock()
	for _, cachingInstance := range chInstanceList {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var chacheIDList []string // will hold coply cache IDs list to avoid concurrency
			tc.offCollector.allCollMux.RLock()
			tc.cache[cachingInstance].RLock()
			tc.offCollector.setCollMux[cachingInstance].RLock()
			defer func() {
				tc.offCollector.setCollMux[cachingInstance].RUnlock()
				tc.cache[cachingInstance].RUnlock()
			}()
			for chIDLst := range tc.offCollector.setColl[cachingInstance] {
				chacheIDList = append(chacheIDList, chIDLst)
			}
			tc.offCollector.allCollMux.RUnlock()
			if err := tc.offCollector.writeRemoveEntity(cachingInstance); err != nil {
				errChan <- err
				return
			}
			for _, cacheID := range chacheIDList {
				// put cache item in new values so we dont lock cache for entire duration of encoding/writing
				value := tc.cache[cachingInstance].cache[cacheID].value
				expiryTime := tc.cache[cachingInstance].cache[cacheID].expiryTime
				groupIDs := tc.cache[cachingInstance].cache[cacheID].groupIDs
				if err := tc.offCollector.writeSetEntity(cachingInstance, cacheID, value,
					expiryTime, groupIDs); err != nil {
					errChan <- err
					return
				}
				delete(tc.offCollector.setColl[cachingInstance], cacheID)
			}
		}()
	}
	go func() {
		wg.Wait()
		done <- struct{}{}
	}()
	select {
	case err := <-errChan:
		return err
	case <-done:
		return nil
	}
}

// Will gather all sets and removes, from dump files and rewrite a new streamlined dump file
func (tc *TransCache) Rewrite() error {
	if tc.offCollector == nil {
		return fmt.Errorf("InternalDB dump not activated")
	}
	tc.offCollector.rewrite()
	return nil
}

// Depending on dump and rewrite intervals, will write all thats left in cache collector to file and/or rewrite dump files, and close all files after
func (tc *TransCache) Shutdown() {
	if tc.offCollector == nil {
		return
	}
	if tc.offCollector.dumpInterval > 0 {
		tc.offCollector.stopWriting <- struct{}{}
		<-tc.offCollector.writeStopped
		if err := tc.WriteAll(); err != nil {
			tc.offCollector.logger.Err(err.Error())
		}
	}
	if tc.offCollector.rewriteInterval > 0 {
		tc.offCollector.stopRewrite <- struct{}{}
		<-tc.offCollector.rewriteStopped
		tc.offCollector.rewrite()
	}
	if tc.offCollector.rewriteInterval == -2 {
		tc.offCollector.rewrite()
	}
	for _, file := range tc.offCollector.files {
		if err := closeFile(file); err != nil {
			tc.offCollector.logger.Err(err.Error())
			continue
		}
	}

}
