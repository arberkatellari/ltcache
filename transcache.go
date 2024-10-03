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
	"crypto/rand"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"golang.org/x/exp/mmap"
)

const (
	AddItem              = "AddItem"
	RemoveItem           = "RemoveItem"
	RemoveGroup          = "RemoveGroup"
	DefaultCacheInstance = "*default"
	GroupsSffx           = "-groups"
)

var (
	ErrNotFound    = errors.New("not found")
	ErrNotClonable = errors.New("not clonable")
	Delimiter      = []byte("\n---") // defines the characters that will be used to seperate cacheItems from each other when dumped to a file
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

// Method to populate DumpTransactionItem struct with values of original transactionItem
func (tItem *transactionItem) toDumpTransactionItem() *DumpTransactionItem {
	return &DumpTransactionItem{
		Verb:     tItem.verb,
		CacheID:  tItem.cacheID,
		ItemID:   tItem.itemID,
		Value:    tItem.value,
		GroupIDs: tItem.groupIDs,
	}
}

// Populate transactionItem with values of recovered DumpTransactionItem
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
	tc.transBufMux.Lock()
	tc.transactionMux.Lock()
	defer func() {
		tc.transBufMux.Unlock()
		tc.transactionMux.Unlock()
	}()
	for transID, transItems := range tc.transactionBuffer {
		var dmpTransBuff []*DumpTransactionItem
		for _, transItem := range transItems {
			dmpTransBuff = append(dmpTransBuff, transItem.toDumpTransactionItem())
		}
		dmpTC.TransactionBuffer[transID] = dmpTransBuff
	}
	return dmpTC
}

// Populate TransCache with values of recovered DumpTransCache
// func newTransCacheFromDump(dmpTC *DumpTransCache, cfg map[string]*CacheConfig) *TransCache {
// 	if _, has := cfg[DefaultCacheInstance]; !has { // Default always created
// 		cfg[DefaultCacheInstance] = &CacheConfig{MaxItems: -1}
// 	}
// 	tc := &TransCache{
// 		cache:             make(map[string]*Cache, len(dmpTC.Cache)),
// 		cfg:               cfg,
// 		transactionBuffer: make(map[string][]*transactionItem, len(dmpTC.TransactionBuffer)),
// 	}

// 	for key, dmpCache := range dmpTC.Cache {
// 		tc.cache[key] = newCacheFromDump(dmpCache, cfg[key].MaxItems, cfg[key].TTL, cfg[key].StaticTTL, cfg[key].OnEvicted)
// 	}
// 	for dumpTransID, dumpTransItem := range dmpTC.TransactionBuffer {
// 		var transBuff []*transactionItem
// 		for _, transItem := range dumpTransItem {
// 			transBuff = append(transBuff, newTransBufferFromDump(transItem))
// 		}
// 		tc.transactionBuffer[dumpTransID] = transBuff
// 	}
// 	return tc
// }

// Will read all data from dump file and put them back on a new TransCache
func ReadAll(fldrPath string, cfg map[string]*CacheConfig) (tc *TransCache, err error) {
	todmpTime1 := time.Now()
	entries, err := os.ReadDir(fldrPath)
	if err != nil {
		return
	}
	fmt.Println("time to readDir: ", time.Since(todmpTime1))
	var fileNames []string // Holds slice of all file names found in folder path
	for _, entry := range entries {
		fileNames = append(fileNames, entry.Name())
	}
	if _, has := cfg[DefaultCacheInstance]; !has { // Default always created
		cfg[DefaultCacheInstance] = &CacheConfig{MaxItems: -1}
	}
	tc = &TransCache{
		cache:             make(map[string]*Cache),
		cfg:               cfg,
		transactionBuffer: make(map[string][]*transactionItem),
	}
	var wg sync.WaitGroup
	errChan := make(chan error, 1) // used to stop and return the function if there are errors
	done := make(chan struct{}, 1)
	var mapMu sync.RWMutex
	var once sync.Once
	for i := range fileNames {
		if strings.HasSuffix(fileNames[i], GroupsSffx) {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			todmpTime1 := time.Now()
			r, err := mmap.Open(fldrPath + "/" + fileNames[i])
			if err != nil {
				once.Do(func() {
					errChan <- err
				})
				// return nil, err
			}
			fmt.Println("time to open file: ", fileNames[i], time.Since(todmpTime1))
			todmpTime1 = time.Now()
			p := make([]byte, r.Len()) // Holds the bytes of the file read
			_, err = r.ReadAt(p, 0)
			if err != nil {
				once.Do(func() {
					errChan <- err
				})
				// return nil, err
			}
			fmt.Println("time to ReadAt: ", fileNames[i], time.Since(todmpTime1))
			todmpTime1 = time.Now()
			scanner := bufio.NewScanner(bytes.NewReader(p)) // Create new bufio scanner out of the file bytes
			fmt.Println("time to NewScanner: ", fileNames[i], time.Since(todmpTime1))
			scanner.Buffer(make([]byte, 0, 1024), 1024*2024) // modify the scanner token limit to 1MiB, default 1KiB. A token in this case would hold 1 cache 1tem
			scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
				dataLen := len(data)
				if atEOF && dataLen == 0 {
					return 0, nil, nil
				}
				// Find next Delimiter and return token
				if i := bytes.Index(data, Delimiter); i >= 0 {
					return i + len(Delimiter), data[0:i], nil
				}
				if atEOF { // Return the entire data when EOF
					return dataLen, data, nil
				}
				return 0, nil, nil
			})

			if fileNames[i] != "TransactionBuffer" &&
				!strings.HasSuffix(fileNames[i], GroupsSffx) {
				mapMu.Lock()
				tc.cache[fileNames[i]] = &Cache{
					cache:      make(map[string]*cachedItem),
					groups:     make(map[string]map[string]struct{}),
					onEvicted:  cfg[fileNames[i]].OnEvicted,
					maxEntries: cfg[fileNames[i]].MaxItems,
					ttl:        cfg[fileNames[i]].TTL,
					staticTTL:  cfg[fileNames[i]].StaticTTL,
					lruIdx:     list.New(),
					lruRefs:    make(map[string]*list.Element),
					ttlIdx:     list.New(),
					ttlRefs:    make(map[string]*list.Element),
				}
				if tc.cache[fileNames[i]].ttl > 0 {
					go tc.cache[fileNames[i]].cleanExpired()
				}
				mapMu.Unlock()
			}
			var ndcTime, dcTBTime, dcgTime, gcTime, dcciTime, pop1cTime, pop1ciTime time.Duration
			var ndcCount, dcTBCount, dcgCount, gcCount, dcciCount, pop1cCount, pop1ciCount int
			for scanner.Scan() {
				todmpTime1 := time.Now()
				decoder := gob.NewDecoder(bytes.NewReader(scanner.Bytes())) // scanner.Bytes() holds the bytes of 1 slice of the file
				ndcTime += time.Since(todmpTime1)
				ndcCount++
				// fmt.Println("time to NewDecoder: ", fileNames[i], time.Since(todmpTime1))
				if fileNames[i] == "TransactionBuffer" { // populate TransCaches's TransactionBuffer
					var transBuf map[string][]*DumpTransactionItem
					todmpTime1 := time.Now()
					err = decoder.Decode(&transBuf)
					dcTBTime += time.Since(todmpTime1)
					dcTBCount++
					// fmt.Println("time to Decode TransBuf: ", fileNames[i], time.Since(todmpTime1))
					if err != nil {
						once.Do(func() {
							errChan <- fmt.Errorf("error decoding TransactionBuffer: %w", err)
						})
						// return nil, fmt.Errorf("error decoding TransactionBuffer: %w", err)
					}
					for dumpTransID, dumpTransItem := range transBuf {
						var transBuff []*transactionItem
						for _, transItem := range dumpTransItem {
							transBuff = append(transBuff, newTransBufferFromDump(transItem))
						}
						tc.transactionBuffer[dumpTransID] = transBuff
					}
					r.Close()
					break
				} /*else if strings.Contains(fileNames[i], GroupsSffx) {
					// populate Cache's Groups
					var groups map[string]map[string]struct{}
					todmpTime1 := time.Now()
					if err := decoder.Decode(&groups); err != nil {
						if err.Error() == "unexpected EOF" || err.Error() == "EOF" {
							r.Close()
							break
						}
						once.Do(func() {
							errChan <- fmt.Errorf("error decoding groups: %w", err)
						})
						// return nil, fmt.Errorf("error decoding groups: %w", err)
					}
					dcgTime += time.Since(todmpTime1)
					dcgCount++
					// fmt.Println("Time to decode groups: ", fileNames[i], time.Since(todmpTime1))
					todmpTime1 = time.Now()
					if _, has := tc.cache[strings.TrimSuffix(fileNames[i], GroupsSffx)]; !has {
						tc.cache[strings.TrimSuffix(fileNames[i], GroupsSffx)] = new(Cache)
					}
					tc.cache[strings.TrimSuffix(fileNames[i], GroupsSffx)].groups = groups
					gcTime += time.Since(todmpTime1)
					gcCount++
					// fmt.Println("Time to populate groups in cache: ", fileNames[i], time.Since(todmpTime1))
					r.Close()
					break
				}*/ // populate TransCaches's CachingInstances and their Cache
				var chIdItmPair ChIDItemPair
				todmpTime1 = time.Now()
				if err := decoder.Decode(&chIdItmPair); err != nil {
					if err.Error() != "unexpected EOF" && err.Error() != "EOF" { // continue if EOF
						once.Do(func() {
							errChan <- fmt.Errorf("error decoding ChIDItemPair: %w", err)
						})
						// return nil, fmt.Errorf("error decoding ChIDItemPair: %w", err)
					}
				} else { // populate Cache's CacheID and Items
					dcciTime += time.Since(todmpTime1)
					dcciCount++
					// fmt.Println("Time to decode 1 chitem pair: ", fileNames[i], time.Since(todmpTime1))
					todmpTime1 := time.Now()
					mapMu.RLock()
					tc.cache[fileNames[i]].cache[chIdItmPair.ChID] = chIdItmPair.Item.toCachedItem()
					mapMu.RUnlock()
					pop1cTime += time.Since(todmpTime1)
					pop1cCount++
					// fmt.Println("Time to populate 1 cache item: ", fileNames[i], time.Since(todmpTime1))
					todmpTime1 = time.Now()
					mapMu.RLock()
					tc.cache[fileNames[i]].set(chIdItmPair.ChID, tc.cache[fileNames[i]].cache[chIdItmPair.ChID])
					mapMu.RUnlock()
					pop1ciTime += time.Since(todmpTime1)
					pop1ciCount++
					// fmt.Println("time to set chache in cache instance: ", fileNames[i], time.Since(todmpTime1))
				}
			}
			r.Close()
			if err := scanner.Err(); err != nil {
				once.Do(func() {
					errChan <- fmt.Errorf("<%w> from file <%v>", err, fileNames[i])
				})
				// return nil, fmt.Errorf("<%w> from file <%v>", err, fileNames[i])
			}
			// (REMOVE LATER) ADD ITEMS COUNT AND AVG TIME FOR 1 DECODE TO COMPLETE
			fmt.Println("aggregated ", ndcCount, ", time to NewDecoder: ", fileNames[i], ndcTime)
			fmt.Println("aggregated ", dcTBCount, ",time to Decode TransBuf: ", fileNames[i], dcTBTime)
			fmt.Println("aggregated ", dcgCount, ",Time to decode groups: ", fileNames[i], dcgTime)
			fmt.Println("aggregated ", gcCount, ",Time to populate groups in cache: ", fileNames[i], gcTime)
			fmt.Println("aggregated ", dcciCount, ",Time to decode 1 chitem pair: ", fileNames[i], dcciTime)
			fmt.Println("aggregated ", pop1cCount, ",Time to populate 1 cache item: ", fileNames[i], pop1cTime)
			fmt.Println("aggregated ", pop1ciCount, ",time to set chache in cache instance: ", fileNames[i], pop1ciTime)
		}()
	}
	go func() {
		wg.Wait()
		for i := range fileNames {
			if !strings.Contains(fileNames[i], GroupsSffx) {
				continue
			}
			todmpTime1 := time.Now()
			r, err := mmap.Open(fldrPath + "/" + fileNames[i])
			if err != nil {
				once.Do(func() {
					errChan <- err
				})
				// return nil, err
			}
			fmt.Println("time to open file: ", fileNames[i], time.Since(todmpTime1))
			todmpTime1 = time.Now()
			p := make([]byte, r.Len()) // Holds the bytes of the file read
			_, err = r.ReadAt(p, 0)
			if err != nil {
				once.Do(func() {
					errChan <- err
				})
				// return nil, err
			}
			fmt.Println("time to ReadAt: ", fileNames[i], time.Since(todmpTime1))
			todmpTime1 = time.Now()
			scanner := bufio.NewScanner(bytes.NewReader(p)) // Create new bufio scanner out of the file bytes
			fmt.Println("time to NewScanner: ", fileNames[i], time.Since(todmpTime1))
			scanner.Buffer(make([]byte, 0, 1024), 1024*2024) // modify the scanner token limit to 1MiB, default 1KiB. A token in this case would hold 1 cache 1tem
			var ndcTime, dcgTime, gcTime time.Duration
			var ndcCount, dcgCount, gcCount int
			for scanner.Scan() {
				todmpTime1 := time.Now()
				decoder := gob.NewDecoder(bytes.NewReader(scanner.Bytes())) // scanner.Bytes() holds the bytes of 1 slice of the file
				ndcTime += time.Since(todmpTime1)
				ndcCount++
				// populate Cache's Groups
				var groups map[string]map[string]struct{}
				todmpTime1 = time.Now()
				if err := decoder.Decode(&groups); err != nil {
					if err.Error() == "unexpected EOF" || err.Error() == "EOF" {
						r.Close()
						break
					}
					once.Do(func() {
						errChan <- fmt.Errorf("error decoding groups: %w", err)
					})
					// return nil, fmt.Errorf("error decoding groups: %w", err)
				}
				dcgTime += time.Since(todmpTime1)
				dcgCount++
				// fmt.Println("Time to decode groups: ", fileNames[i], time.Since(todmpTime1))
				todmpTime1 = time.Now()
				tc.cache[strings.TrimSuffix(fileNames[i], GroupsSffx)].groups = groups
				gcTime += time.Since(todmpTime1)
				gcCount++
				// fmt.Println("Time to populate groups in cache: ", fileNames[i], time.Since(todmpTime1))
				r.Close()
				break
			}
			r.Close()
			if err := scanner.Err(); err != nil {
				once.Do(func() {
					errChan <- fmt.Errorf("<%w> from file <%v>", err, fileNames[i])
				})
				// return nil, fmt.Errorf("<%w> from file <%v>", err, fileNames[i])
			}
			// (REMOVE LATER) ADD ITEMS COUNT AND AVG TIME FOR 1 DECODE TO COMPLETE
			fmt.Println("aggregated ", ndcCount, ", time to NewDecoder: ", fileNames[i], ndcTime)
			fmt.Println("aggregated ", dcgCount, ",Time to decode groups: ", fileNames[i], dcgTime)
			fmt.Println("aggregated ", gcCount, ",Time to populate groups in cache: ", fileNames[i], gcTime)
		}
		done <- struct{}{}
	}()
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
	select {
	case err := <-errChan:
		return nil, err
	case <-done:
		return
	}
	// return
}

// WriteAll will write all of TransCache in a dump file
func (tc *TransCache) WriteAll(fldrPath string) error {
	staratWrite := time.Now()
	defer func() {
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

	todmpTime1 := time.Now()
	tcDmp := tc.toDumpTransCache() // Holds the TransCache converted to DumpTransCache to preserve types of unexportable fields
	fmt.Println("writeTime", time.Since(todmpTime1))
	var wg sync.WaitGroup
	errChan := make(chan error, 1) // used to stop and return the function if there are errors
	var once sync.Once             // used with errChan to  store only the first error
	for chInstance, dumpCache := range tcDmp.Cache {
		if chInstance == "*reverse_destinations" {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			todmpTime1 := time.Now()
			if err := writeDumpCacheToFile(fldrPath+"/"+chInstance, dumpCache); err != nil {
				once.Do(func() {
					errChan <- err
				})
				return
			}
			fmt.Println("1cachetofile: ", chInstance, time.Since(todmpTime1))
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		todmpTime1 := time.Now()
		if err := writeDumpTransBufferToFile(fldrPath+"/TransactionBuffer",
			tcDmp.TransactionBuffer); err != nil {
			once.Do(func() {
				errChan <- err
			})
		}
		fmt.Println("1transtofile", time.Since(todmpTime1))
	}()
	wg.Wait()
	select {
	case err := <-errChan:
		return err
	default:
		return nil
	}
}

// Will encode dmpTransBuf and write it to a file
func writeDumpTransBufferToFile(fileName string, dmpTransBuf map[string][]*DumpTransactionItem) (err error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	file, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()
	if err = encoder.Encode(dmpTransBuf); err != nil {
		return fmt.Errorf("failed encoding data: %w", err)
	}
	fbuff := bufio.NewWriter(file)
	if _, err = buf.WriteTo(fbuff); err != nil {
		return fmt.Errorf("failed writing to file: %w", err)
	}
	if err = fbuff.Flush(); err != nil {
		return fmt.Errorf("failed flushing buffer: %w", err)
	}
	return
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

// Get returns the value of an Itemm, NOT THREAD SAFE
func (tc *TransCache) GetUnsafe(chID, itmID string) (interface{}, bool) {
	return tc.cacheInstance(chID).GetUnsafe(itmID)
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

// Set will add/edit an item to the cache, NOT THREAD SAFE
func (tc *TransCache) SetUnsafe(chID, itmID string, value interface{},
	groupIDs []string, commit bool, transID string) {
	if commit {
		tc.cacheInstance(chID).SetUnsafe(itmID, value, groupIDs)
	} else {
		tc.transactionBuffer[transID] = append(tc.transactionBuffer[transID],
			&transactionItem{cacheID: chID,
				verb: AddItem, itemID: itmID,
				value: value, groupIDs: groupIDs})
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

// GetItemIDs returns a list of item IDs matching prefix, NOT THREAD SAFE
func (tc *TransCache) GetItemIDsUnsafe(chID, prfx string) (itmIDs []string) {
	itmIDs = tc.cacheInstance(chID).GetItemIDsUnsafe(prfx)
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

// HasItem verifies if Item is in the cache, NOT THREAD SAFE
func (tc *TransCache) HasItemUnsafe(chID, itmID string) (has bool) {
	has = tc.cacheInstance(chID).HasItemUnsafe(itmID)
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
