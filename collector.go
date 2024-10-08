/*
TransCache is released under the MIT License <http://www.opensource.org/licenses/mit-license.php
Copyright (C) ITsysCOM GmbH. All Rights Reserved.

TransCache is a bigger version of Cache with support for multiple Cache instances and transactions
*/

package ltcache

import (
	"bufio"
	"bytes"
	"container/list"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"golang.org/x/exp/mmap"
)

// Used to temporarily hold caching instances, until dumped to file
type InstanceCollector struct {
	collMux        sync.Mutex                            // locks struct when editing dump files
	SetColl        map[string]map[string]*CacheCollector // map[cachingInstance]map[cacheItemKey]*item   Collects all key-values SET on cache
	RemColl        map[string][]string                   // map[cachingInstance][]cacheItemKey Collects all keys to be removed from files
	folderPath     string                                // path to the database dump folder
	writeOnArrival bool                                  // write in file on each set/remove
	isActive       bool                                  // used on cache set/remove to know if we want
	// to dump to file or not
}

// Used to temporarily hold cache items in memory, until dumped to file
type CacheCollector struct {
	Value      any
	GroupIDs   []string
	ExpiryTime time.Time
}

// Reads from files to create TransCache
func ReadAll(fldrPath string, cfg map[string]*CacheConfig) (tc *TransCache, err error) {
	if _, has := cfg[DefaultCacheInstance]; !has { // Default always created
		cfg[DefaultCacheInstance] = &CacheConfig{MaxItems: -1}
	}
	tc = &TransCache{
		cache:             make(map[string]*Cache),
		cfg:               cfg,
		transactionBuffer: make(map[string][]*transactionItem),
		Collector: &InstanceCollector{
			SetColl:    make(map[string]map[string]*CacheCollector),
			isActive:   true,
			folderPath: fldrPath,
		},
	}
	entries, err := os.ReadDir(fldrPath)
	if err != nil {
		for cacheID, chCfg := range cfg {
			onEv := func(itemID string, _ any) { // put cacheIDs on RemColl when they are deleted from cache/expiry (onEvicted)
				tc.Collector.collMux.Lock()
				tc.Collector.RemColl[cacheID] = append(tc.Collector.RemColl[cacheID], itemID)
				tc.Collector.collMux.Unlock()
			}
			tc.cache[cacheID] = NewCache(chCfg.MaxItems, chCfg.TTL, chCfg.StaticTTL, onEv)
		}
		return
	}
	var fileNames []string // Holds slice of all file names found in folder path
	for _, entry := range entries {
		fileNames = append(fileNames, entry.Name())
	}

	var wg sync.WaitGroup
	errChan := make(chan error, 1) // used to stop and return the function if there are errors
	done := make(chan struct{}, 1)
	var mapMu sync.RWMutex
	var once sync.Once
	for i := range fileNames {
		wg.Add(1)
		go func() {
			defer wg.Done()
			todmpTime1 := time.Now()
			r, err := mmap.Open(fldrPath + "/" + fileNames[i])
			if err != nil {
				once.Do(func() {
					errChan <- err
					// return nil, err
				})
				return
			}
			fmt.Println(fileNames[i], " time to open in memory: ", time.Since(todmpTime1))
			p := make([]byte, r.Len()) // Holds the bytes of the file read
			todmpTime1 = time.Now()
			_, err = r.ReadAt(p, 0)
			if err != nil {
				once.Do(func() {
					errChan <- err
					// return nil, err
				})
				return
			}
			fmt.Println(fileNames[i], " time to put in bytes: ", time.Since(todmpTime1))
			todmpTime1 = time.Now()
			reader := bufio.NewReader(bytes.NewReader(p))
			dec := gob.NewDecoder(reader)
			fmt.Println(fileNames[i], " time to NewDecoder: ", time.Since(todmpTime1))
			cacheColl := make(map[string]*CacheCollector)
			todmpTime1 = time.Now()
			for {
				var kv Kv
				if err := dec.Decode(&kv); err != nil {
					// if err := dec.Decode(&cacheColl); err != nil {
					break
					// if err == io.EOF {
					// 	break // End of file
					// }
					// once.Do(func() {
					// 	errChan <- fmt.Errorf("failed to decode CacheColl: %w", err)
					// 	// return fmt.Errorf("failed to decode CacheColl: %w", err)
					// })
					// return
				}
				cacheColl[kv.K] = kv.V
			}
			if err != nil && err != io.EOF {
				once.Do(func() {
					errChan <- fmt.Errorf("failed to decode kv: %w", err)
					// return fmt.Errorf("failed to decode CacheColl: %w", err)
				})
			}
			fmt.Println(fileNames[i], " time to decode: ", time.Since(todmpTime1))
			mapMu.Lock()
			todmpTime1 = time.Now()
			onEv := func(itemID string, _ any) { // put cacheIDs on RemColl when they are deleted from cache from expiry (onEvicted)
				tc.Collector.RemColl[fileNames[i]] = append(tc.Collector.RemColl[fileNames[i]], itemID)
			}
			tc.cache[fileNames[i]] = newCacheFromDump(cacheColl, cfg[fileNames[i]].MaxItems, cfg[fileNames[i]].TTL, cfg[fileNames[i]].StaticTTL, onEv)
			fmt.Println(fileNames[i], " time to newCacheFromDump: ", time.Since(todmpTime1))
			mapMu.Unlock()
		}()
	}
	go func() {
		wg.Wait()
		for cacheID, chCfg := range cfg {
			if tc.cache[cacheID] == nil {
				onEv := func(itemID string, _ any) { // put cacheIDs on RemColl when they are deleted from cache from expiry (onEvicted)
					tc.Collector.RemColl[cacheID] = append(tc.Collector.RemColl[cacheID], itemID)
				}
				tc.cache[cacheID] = NewCache(chCfg.MaxItems, chCfg.TTL, chCfg.StaticTTL, onEv)
			}
		}
		done <- struct{}{}
	}()
	select {
	case err := <-errChan:
		return nil, err
	case <-done:
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
		return
	}
}

// Method to populate cachedItem with values of recovered DumpCachedItem
func (cc *CacheCollector) toCachedItem(chItemID string) *cachedItem {
	return &cachedItem{
		itemID:     chItemID,
		value:      cc.Value,
		expiryTime: cc.ExpiryTime,
		groupIDs:   cc.GroupIDs,
	}
}

// set adds lru/ttl indexes and refs for each cachedItem. Used only for recovering from dump (not thread safe)
func (c *Cache) set(chID string, cItem *cachedItem) {
	if c.maxEntries == DisabledCaching {
		return
	}
	if c.maxEntries != UnlimitedCaching {
		c.lruRefs[chID] = c.lruIdx.PushFront(cItem)
	}
	if c.ttl > 0 {
		c.ttlRefs[chID] = c.ttlIdx.PushFront(cItem)
	}
	if c.maxEntries != UnlimitedCaching {
		var lElm *list.Element
		if c.lruIdx.Len() > c.maxEntries {
			lElm = c.lruIdx.Back()
		}
		if lElm != nil {
			c.remove(lElm.Value.(*cachedItem).itemID)
		}
	}
}

// Populate Cache with values of recovered cache
func newCacheFromDump(cacheColl map[string]*CacheCollector, maxEntries int, ttl time.Duration, staticTTL bool,
	onEvicted func(itmID string, value interface{})) *Cache {
	cache := &Cache{
		cache:      make(map[string]*cachedItem),
		groups:     make(map[string]map[string]struct{}),
		onEvicted:  onEvicted,
		maxEntries: maxEntries,
		ttl:        ttl,
		staticTTL:  staticTTL,
		lruIdx:     list.New(),
		lruRefs:    make(map[string]*list.Element),
		ttlIdx:     list.New(),
		ttlRefs:    make(map[string]*list.Element),
	}

	for chID, item := range cacheColl {
		cache.cache[chID] = item.toCachedItem(chID)
		cache.set(chID, cache.cache[chID])
	}
	for groupID, items := range cacheColl {
		cache.addItemToGroups(groupID, items.GroupIDs)
	}
	if cache.ttl > 0 {
		go cache.cleanExpired()
	}
	return cache
}

// Temporarily collects caching instances on each set/remove to be dumped to file later on
func (coll *InstanceCollector) collect(cacheInstance, cacheID string, value any, expiryTime time.Time,
	groupIDs []string) {
	coll.collMux.Lock()
	defer coll.collMux.Unlock()
	if coll.SetColl[cacheInstance] == nil {
		coll.SetColl[cacheInstance] = make(map[string]*CacheCollector)
	}
	coll.SetColl[cacheInstance][cacheID] = &CacheCollector{
		Value:      value,
		ExpiryTime: expiryTime,
		GroupIDs:   groupIDs,
	}
}

// Clears a cache instance from the cache InstanceCollector
func (coll *InstanceCollector) clearCacheInstance(cacheInstance string) {
	coll.collMux.Lock()
	defer coll.collMux.Unlock()
	// clear setColl and RemColl collected in memory
	coll.SetColl[cacheInstance] = make(map[string]*CacheCollector)
	coll.RemColl[cacheInstance] = make([]string, 0)
	// remove the dump file if exists
	if _, err := os.Stat(coll.folderPath + "/" + cacheInstance); err == nil && os.IsExist(err) {
		os.Remove(coll.folderPath + "/" + cacheInstance)
	}
}

// Will delete cache items set in the setCollector correlating to the cacheIDs set on remCollector
func removeKeysFromSetCollector(remColl map[string][]string, setColl map[string]map[string]*CacheCollector) {
	for cachingInstance, removeKeys := range remColl {
		for _, key := range removeKeys {
			if _, has := setColl[cachingInstance]; has {
				delete(setColl[cachingInstance], key)
			}
		}
	}
}

type Kv struct {
	K string
	V *CacheCollector
}

// Dumps all of InstanceCollectors in files
func (coll *InstanceCollector) WriteAll() error {
	coll.collMux.Lock()
	defer coll.collMux.Unlock()

	removeKeysFromSetCollector(coll.RemColl, coll.SetColl)

	var wg sync.WaitGroup
	errChan := make(chan error, 1) // used to stop and return the function if there are errors
	done := make(chan struct{}, 1)
	var once sync.Once
	for cachingInstance := range coll.SetColl {
		wg.Add(1)
		go func() {
			defer wg.Done()
			filePath := coll.folderPath + "/" + cachingInstance
			// Check if the file exists
			var file *os.File
			if _, err := os.Stat(filePath); err == nil {
				// File exists, open it for writing
				file, err = os.OpenFile(filePath, os.O_RDWR, 0644)
				if err != nil {
					once.Do(func() {
						errChan <- fmt.Errorf("failed to open existing file %s: %w", filePath, err)
						// return fmt.Errorf("failed to open existing file %s: %w", filePath, err)
					})
					return
				}
				defer file.Close()
				fmt.Println(cachingInstance, "file opened for writing (existing)")
			} else if os.IsNotExist(err) {
				// File does not exist, create it
				file, err = os.Create(filePath)
				if err != nil {
					once.Do(func() {
						errChan <- fmt.Errorf("failed to create file %s: %w", filePath, err)
						// return fmt.Errorf("failed to create file %s: %w", filePath, err)
					})
					return
				}
				defer file.Close()
				fmt.Println(cachingInstance, "file created and opened")
			} else {
				once.Do(func() {
					errChan <- fmt.Errorf("error checking file existence for %s: %w", filePath, err)
					// return fmt.Errorf("error checking file existence for %s: %w", filePath, err)
				})
				return
			}

			todmpTime1 := time.Now()
			writer := bufio.NewWriter(file)
			enc := gob.NewEncoder(writer)
			fmt.Println(cachingInstance, " time to NewEncoder:", time.Since(todmpTime1))
			todmpTime1 = time.Now()
			for k, v := range coll.SetColl[cachingInstance] {
				if err := enc.Encode(Kv{K: k, V: v}); err != nil {
					// if err := enc.Encode(coll.SetColl[cachingInstance]); err != nil {
					once.Do(func() {
						errChan <- fmt.Errorf("failed to encode CacheColl for %s: %w", cachingInstance, err)
						// return fmt.Errorf("failed to encode CacheColl for %s: %w", cachingInstance, err)
					})
					return
				}
			}
			if err := writer.Flush(); err != nil {
				once.Do(func() {
					errChan <- fmt.Errorf("failed to flush writer for %s: %w", cachingInstance, err)
				})
				return
			}
			coll.SetColl[cachingInstance] = make(map[string]*CacheCollector)
			fmt.Println(cachingInstance, " time to Encode:", time.Since(todmpTime1))
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
		// remove written collection from memory
		coll.SetColl = make(map[string]map[string]*CacheCollector)
		return nil
	}
}
