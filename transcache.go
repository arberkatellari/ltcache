/*
TransCache is released under the MIT License <http://www.opensource.org/licenses/mit-license.php
Copyright (C) ITsysCOM GmbH. All Rights Reserved.

TransCache is a bigger version of Cache with support for multiple Cache instances and transactions
*/

package ltcache

import (
	"container/list"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
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

type CacheConfig struct {
	MaxItems  int
	TTL       time.Duration
	StaticTTL bool
	OnEvicted func(itmID string, value interface{})
}

// used for shutdown options when using offlineCollector
type offlineHelper struct {
	dumpInterval    time.Duration // holds duration to wait until next write
	stopWriting     chan struct{} // Used to stop inverval writing
	writeStopped    chan struct{} // signal when writing is finished
	rewriteInterval time.Duration // holds duration to wait until next rewrite
	stopRewrite     chan struct{} // Used to stop inverval rewriting
	rewriteStopped  chan struct{} // signal when rewriting is finished
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

	offlineHelper offlineHelper // information needed for TransCache shutdown process when using offlineCollector
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
		offlineHelper: offlineHelper{
			dumpInterval:    dumpInterval,
			rewriteInterval: rewriteInterval,
			stopWriting:     make(chan struct{}),
			writeStopped:    make(chan struct{}),
			stopRewrite:     make(chan struct{}),
			rewriteStopped:  make(chan struct{}),
		},
	}
	var wg sync.WaitGroup
	errChan := make(chan error, 1)
	done := make(chan struct{})
	for chInstance, config := range tc.cfg {
		if err := ensureDir(path.Join(fldrPath, chInstance)); err != nil {
			return nil, err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			cache, err := newCacheFromReader(chInstance, path.Join(fldrPath, chInstance),
				config.MaxItems, config.TTL, config.StaticTTL, l, writeLimit, dumpInterval)
			if err != nil {
				errChan <- err
				return
			}
			tc.cacheMux.Lock() // avoid locking all of tc.cache map while the caching instance isn't constructed yet
			tc.cache[chInstance] = cache
			tc.cacheMux.Unlock()
		}()
	}
	go func() {
		wg.Wait()
		if rewriteInterval == -1 {
			tc.RewriteAll()
		}
		close(done)
	}()

	select {
	case err := <-errChan:
		return nil, err
	case <-done:
		if dumpInterval > 0 {
			go tc.asyncWriteEntities()
		}
		if rewriteInterval > 0 {
			go tc.asyncRewriteEntities()
		}
		return tc, nil
	}
}

// Read dump files per caching instance and recover the TransCache.cache map from it
func newCacheFromReader(chInstance, instanceFldrPath string, maxEntries int, ttl time.Duration, staticTTL bool, l logger, writeLimit int, dumpInterval time.Duration) (*Cache, error) {
	filePaths, err := getFilePaths(instanceFldrPath)
	if err != nil {
		return nil, fmt.Errorf("error walking the path: %w", err)
	}
	paths, err := validateFilePaths(filePaths, instanceFldrPath)
	if err != nil {
		return nil, err
	}
	cache := &Cache{
		cache:      make(map[string]*cachedItem),
		groups:     make(map[string]map[string]struct{}),
		maxEntries: maxEntries,
		ttl:        ttl,
		staticTTL:  staticTTL,
		lruIdx:     list.New(),
		lruRefs:    make(map[string]*list.Element),
		ttlIdx:     list.New(),
		ttlRefs:    make(map[string]*list.Element),
		offCollector: &OfflineCollector{
			collection:       make(map[string]*CollectionEntity),
			chInstance:       chInstance,
			instanceFldrPath: instanceFldrPath,
			writeLimit:       writeLimit,
			collectSet:       dumpInterval != -1,
			logger:           l,
		},
	}
	cache.onEvicted = func(itemID string, _ any) { // ran when an item is removed from cache
		cache.offCollector.storeRemoveEntity(itemID, dumpInterval)
	}
	instance, err := readInstanceFromFiles(paths)
	if err != nil {
		return nil, err
	}
	for _, entity := range instance {
		cache.cache[entity.ItemID] = entity.toCachedItem()
		cache.set(entity.ItemID, cache.cache[entity.ItemID])
		cache.addItemToGroups(entity.ItemID, entity.GroupIDs)
	}
	if cache.ttl > 0 {
		go cache.cleanExpired()
	}
	if err := cache.offCollector.populateEncoder(); err != nil {
		return nil, err
	}
	return cache, nil
}

// gathers all entities saved in an instance folder
func readInstanceFromFiles(paths []string) (map[string]*OfflineCacheEntity, error) {
	instance := make(map[string]*OfflineCacheEntity) // momentarily holds only necessary entities of all files of caching instance until they are put in Cache. Needed so we don’t set something in cache and remove it from cache again when it is read on another file.
	for _, filepath := range paths {                 // ranges over all files inside instance
		if err := readAndDecodeFile(filepath, instance); err != nil {
			return nil, err
		}
	}
	return instance, nil
}

// Write the OfflineCollection cache items on file every dumpInterval
func (tc *TransCache) asyncWriteEntities() {
	for {
		select {
		case <-tc.offlineHelper.stopWriting: // in case engine is shutdown before interval, dont wait for it
			tc.WriteAll()
			tc.offlineHelper.writeStopped <- struct{}{}
			return
		case <-time.After(tc.offlineHelper.dumpInterval): // no need to instantly write right after reading from files
			tc.WriteAll()
		}
	}
}

// Rewrite dump files on every rewriteInterval
func (tc *TransCache) asyncRewriteEntities() {
	for {
		select {
		case <-tc.offlineHelper.stopRewrite: // in case engine is shutdown before interval, dont wait for it
			tc.RewriteAll()
			tc.offlineHelper.rewriteStopped <- struct{}{}
			return
		case <-time.After(tc.offlineHelper.rewriteInterval): // no need to instantly write right after reading from files
			tc.RewriteAll()
		}
	}
}

// Writes all of collected cache in files
func (tc *TransCache) WriteAll() {
	if tc.offlineHelper.dumpInterval == 0 {
		return
	}
	var wg sync.WaitGroup
	for _, cache := range tc.cache {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := cache.writeToFile(); err != nil {
				cache.offCollector.logger.Err(err.Error())
			}
		}()
	}
	wg.Wait()
}

// Will gather all sets and removes, from files and rewrite a new streamlined file
func (tc *TransCache) RewriteAll() {
	if tc.offlineHelper.dumpInterval == 0 ||
		tc.offlineHelper.rewriteInterval == 0 {
		return
	}
	var wg sync.WaitGroup
	for _, cache := range tc.cache {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cache.offCollector.rewriteFiles()
		}()
	}
	wg.Wait()
}

// Depending on write and rewrite intervals, will write all thats left in cache collector to file and/or rewrite files, and close all files
func (tc *TransCache) Shutdown() {
	if tc.offlineHelper.dumpInterval == 0 {
		return
	}
	if tc.offlineHelper.dumpInterval > 0 {
		tc.offlineHelper.stopWriting <- struct{}{}
		<-tc.offlineHelper.writeStopped
	}
	if tc.offlineHelper.rewriteInterval > 0 {
		tc.offlineHelper.stopRewrite <- struct{}{}
		<-tc.offlineHelper.rewriteStopped
	}
	if tc.offlineHelper.rewriteInterval == -2 {
		tc.RewriteAll()
	}
	for _, cache := range tc.cache {
		if err := closeFile(cache.offCollector.file); err != nil {
			cache.offCollector.logger.Err(err.Error())
		}
	}
}

// Close opened file and delete if empty
func closeFile(file *os.File) error {
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("error getting stats for file <%s>: %w", file.Name(), err)

	}
	file.Close()
	if info.Size() == 0 { // if file isnt populated, delete it
		if err := os.Remove(file.Name()); err != nil {
			return fmt.Errorf("error removing file <%s>: %w", file.Name(), err)
		}
	}
	return nil
}
