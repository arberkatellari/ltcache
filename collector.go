/*
TransCache is released under the MIT License <http://www.opensource.org/licenses/mit-license.php
Copyright (C) ITsysCOM GmbH. All Rights Reserved.

TransCache is a bigger version of Cache with support for multiple Cache instances and transactions
*/

package ltcache

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/exp/mmap"
)

const (
	rewriteFileName = "0Rewrite"   // prefix of the name used for files that have been rewritten, starting with 0 to give natural directory walking priority
	tmpRewriteName  = "tmpRewrite" // prefix of the name of files which are in the process of being rewritten
	oldRewriteName  = "oldRewrite" // prefix of the name of files to be deleted after renewing rewrite files
)

// Used to temporarily hold caching instances, until dumped to file
type OfflineCollector struct {
	collMux          sync.RWMutex                 // lock collection so we dont dump while modifying them
	rewriteMux       sync.RWMutex                 // lock rewriting process
	fileMux          sync.RWMutex                 // used to lock the maps of files, writers and encoders, so we dont have concurrency while writing/reading
	collection       map[string]*CollectionEntity // map[cacheItemKey]*CollectionEntity  Collects all key-values SET/REMOVE-d from cache
	instanceFldrPath string                       // path to a database instance dump folder
	collectSet       bool                         // decides weather to collect or write the SET cache command
	file             *os.File                     // holds the file opened
	writer           *bufio.Writer                // holds the buffer writers, used to flush after writing
	encoder          *gob.Encoder                 // holds encoder
	writeLimit       int                          // maximum size in MiB that can be written in a singular dump file
	chInstance       string                       // holds the name of the cache instance
	logger           logger
}

// Used to temporarily collect cache keys of the items to be dumped to file
type CollectionEntity struct {
	IsSet  bool   // Controls if the item that is collected is a SET or a REMOVE of the item from cache
	ItemID string // Holds the cache ItemID
}

// Used as the structure to be encoded/decoded per cache item to be dumped to file
type OfflineCacheEntity struct {
	IsSet      bool      // Controls if the item that is written is a SET or a REMOVE of the item
	ItemID     string    // Holds the cache ItemID to be stored in file
	Value      any       // Value of cache item to be stored in file
	GroupIDs   []string  // GroupIDs of cache item to be stored in file
	ExpiryTime time.Time // ExpiryTime of cache item to be stored in file
}

type logger interface {
	Alert(string) error
	Close() error
	Crit(string) error
	Debug(string) error
	Emerg(string) error
	Err(string) error
	Info(string) error
	Notice(string) error
	Warning(string) error
}

type nopLogger struct{}

func (nopLogger) Alert(string) error   { return nil }
func (nopLogger) Close() error         { return nil }
func (nopLogger) Crit(string) error    { return nil }
func (nopLogger) Debug(string) error   { return nil }
func (nopLogger) Emerg(string) error   { return nil }
func (nopLogger) Err(string) error     { return nil }
func (nopLogger) Info(string) error    { return nil }
func (nopLogger) Notice(string) error  { return nil }
func (nopLogger) Warning(string) error { return nil }

// Create Directories from path if they dont exist
func ensureDir(path string) error {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return os.MkdirAll(path, 0755)
	}
	return err
}

// open/create dump file, create an encoder and writer for it and store them in the OfflineCollector
func (coll *OfflineCollector) populateEncoder() error {
	filePath := filepath.Join(coll.instanceFldrPath,
		strconv.FormatInt(time.Now().UnixMilli(), 10)) // path of the dump file of current caching instance, in miliseconds in case another dump happens within the second of the dump file created
	var err error
	coll.file, err = os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	coll.writer = bufio.NewWriter(coll.file)
	coll.encoder = gob.NewEncoder(coll.writer)
	return nil
}

// make sure we dont recover from files that were stopped mid way rewriting
func validateFilePaths(paths []string, fileName string) (validPaths []string, err error) {
	// if there are paths with "oldRewrite" prefix, recover from them instead of 0Rewrite
	// having an oldRewrite still in the tree means the rewriting process was interupted
	var removeZeroRewrite bool // true if prefix oldRewrite was found in name of files
	for _, s := range paths {
		if strings.HasPrefix(s, path.Join(fileName, oldRewriteName)) {
			removeZeroRewrite = true
			break
		}
	}
	for _, s := range paths {
		// dont include "tmpRewrite" paths
		if strings.HasPrefix(s, path.Join(fileName, tmpRewriteName)) {
			if err := os.Remove(s); err != nil {
				return nil, err
			}
			continue
		}
		// dont include"0Rewrite" files if any "oldRewrite" found in tree
		if removeZeroRewrite && strings.HasPrefix(s, path.Join(fileName, rewriteFileName)) {
			if err := os.Remove(s); err != nil {
				return nil, err
			}
			continue
		}
		validPaths = append(validPaths, s)
	}
	return
}

// WalkDir and get all file paths on that directory
func getFilePaths(dir string) ([]string, error) {
	var filePaths []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		filePaths = append(filePaths, path)
		return nil
	})
	return filePaths, err
}

// Read dump file and decode
func readAndDecodeFile(filepath string, instance map[string]*OfflineCacheEntity) error {
	r, err := mmap.Open(filepath) // open mmap reader
	if err != nil {
		return fmt.Errorf("error opening file <%s> in memory: %w", filepath, err)
	}
	defer r.Close()
	p := make([]byte, r.Len()) // read into byte slice
	if _, err = r.ReadAt(p, 0); err != nil {
		return fmt.Errorf("error reading file <%s> in memory: %w", filepath, err)
	}
	dec := gob.NewDecoder(bufio.NewReader(bytes.NewReader(p)))
	for {
		var oce *OfflineCacheEntity
		if err := dec.Decode(&oce); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to decode OfflineCacheEntity at <%s>: %w", filepath, err)
		}
		// If the decoded OfflineCacheEntity is a SET command, populate momentary instance map with it
		if oce.IsSet {
			instance[oce.ItemID] = oce
		} else { // If the decoded OfflineCacheEntity is a REMOVE command, remove key from momentary instance map
			delete(instance, oce.ItemID)
		}
	}
	return nil
}

// Method to populate cachedItem with values of recovered OfflineCacheEntity
func (oce *OfflineCacheEntity) toCachedItem() *cachedItem {
	return &cachedItem{
		itemID:     oce.ItemID,
		value:      oce.Value,
		expiryTime: oce.ExpiryTime,
		groupIDs:   oce.GroupIDs,
	}
}

// Collects caching items on each set/remove to be dumped to file later on
func (coll *OfflineCollector) collect(itemID string) {
	coll.collMux.Lock()
	coll.collection[itemID] = &CollectionEntity{
		IsSet:  true,
		ItemID: itemID,
	}
	coll.collMux.Unlock()
}

// encodes OfflineCacheEntity, and writes it to file
func encodeAndWrite(oce OfflineCacheEntity, enc *gob.Encoder, w *bufio.Writer) error {
	if err := enc.Encode(&oce); err != nil {
		return fmt.Errorf("encode error: <%w>", err)
	}
	if err := w.Flush(); err != nil {
		return fmt.Errorf("write error: <%w>", err)
	}
	return nil
}

// checkAndRotateFile checks the size of the file and rotates it if it exceeds the limit.
func (coll *OfflineCollector) checkAndRotateFile() error {
	if coll.writeLimit == -1 {
		return nil
	}
	coll.fileMux.Lock()
	defer coll.fileMux.Unlock()
	fileStat, err := coll.file.Stat()
	if err != nil {
		return fmt.Errorf("error getting file stat: %w", err)
	}
	if fileStat.Size() > int64(coll.writeLimit)*1024*1024 {
		if err := coll.file.Close(); err != nil {
			return fmt.Errorf("error closing file: %w", err)
		}
		if err := coll.populateEncoder(); err != nil {
			return err
		}
	}
	return nil
}

// Writes SET or REMOVE entity on file
func (coll *OfflineCollector) writeEntity(oce OfflineCacheEntity) error {
	if err := coll.checkAndRotateFile(); err != nil {
		return err
	}
	coll.fileMux.Lock()
	defer coll.fileMux.Unlock()
	if err := encodeAndWrite(oce, coll.encoder, coll.writer); err != nil {
		coll.logger.Err("Failed to write cache item for <" + coll.chInstance + ">: " + err.Error())
		return err
	}
	return nil
}

// Writes the REMOVE-d Cache item on file or collects REMOVE entities
func (coll *OfflineCollector) storeRemoveEntity(itemID string, dumpInterval time.Duration) {
	if dumpInterval == -1 {
		if err := coll.writeEntity(OfflineCacheEntity{ItemID: itemID}); err != nil {
			coll.logger.Err(err.Error())
			return
		}
		return
	}
	coll.collMux.Lock()
	coll.collection[itemID] = &CollectionEntity{ItemID: itemID}
	coll.collMux.Unlock()
}

// Will gather all sets and removes, from dump files and rewriteFiles a new streamlined dump file
func (coll *OfflineCollector) rewriteFiles() {
	coll.rewriteMux.Lock()
	defer coll.rewriteMux.Unlock()
	filePaths, instance, skip, err := coll.getFilePathsAndInstance()
	if skip {
		return
	}
	if err != nil {
		coll.logger.Err(err.Error())
		return
	}
	tmpRewritePath := path.Join(coll.instanceFldrPath, tmpRewriteName)   // temporary path to rewrite file
	zeroRewritePath := path.Join(coll.instanceFldrPath, rewriteFileName) // path to completed rewrite file, named 0Rewrite so it stays always first in order of reading files
	oldRewritePath := path.Join(coll.instanceFldrPath, oldRewriteName)   // path to old 0Rewrite file renamed to oldRewrite
	file, err := os.OpenFile(tmpRewritePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		coll.logger.Err("Error opening file <" + tmpRewritePath + ">: " + err.Error())
		return
	}
	tmpFilePaths := []string{tmpRewritePath}
	defer func() { // delete tmpRewrite files if any errors while rewriting so that we dont try to recover from them
		if err != nil {
			file.Close()
			for i := range tmpFilePaths {
				if err := os.Remove(tmpFilePaths[i]); err != nil {
					coll.logger.Err("Failed to remove tmp rewritten file <" + tmpFilePaths[i] + ">, error: " + err.Error())
				}
			}
		}
	}()
	writer := bufio.NewWriter(file)
	enc := gob.NewEncoder(writer)
	for _, oce := range instance {
		if coll.writeLimit > 0 {
			fileStat, _ := file.Stat()
			if fileStat.Size() > int64(coll.writeLimit)*1024*1024 {
				if err := file.Close(); err != nil {
					coll.logger.Err("Error closing file: " + err.Error())
					return
				}
				filePath := tmpRewritePath + strconv.FormatInt(time.Now().UnixMilli(), 10)
				tmpFilePaths = append(tmpFilePaths, filePath)
				file, err = os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					coll.logger.Err("Error opening file <" + filePath + ">: " + err.Error())
					return
				}
				writer = bufio.NewWriter(file)
				enc = gob.NewEncoder(writer)
			}
		}
		if err := encodeAndWrite(*oce, enc, writer); err != nil {
			coll.logger.Err(fmt.Sprintf("Rewrite failed. OfflineCacheEntity <%+v> \nError <%v>", oce, err))
			return
		}
	}
	file.Close()
	// Rename old 0Rewrite to oldRewrite if exists
	for i := range filePaths {
		if strings.Contains(filePaths[i], zeroRewritePath) {
			if err = os.Rename(filePaths[i], oldRewritePath+strconv.Itoa(i)); err != nil {
				coll.logger.Err("Failed to rename file from <" + zeroRewritePath + "> to <" + oldRewritePath + strconv.Itoa(i) + ">: " + err.Error())
				return
			}
			filePaths[i] = oldRewritePath + strconv.Itoa(i)
		}
	}
	// Rename TMPRewrite to 0Rewrite
	for i := range tmpFilePaths {
		// rename so that we can keep the order but also make it unique from rewrite to rewrite to avoid accidental deleting
		index := fmt.Sprintf(fmt.Sprintf("%%0%dd", len(strconv.Itoa(len(tmpFilePaths)))), i) // account for a maximum of digit number of iterations so we keep the order of the files
		zeroRPath := zeroRewritePath + index + "_" + strconv.FormatInt(time.Now().UnixMilli(), 10)
		if err = os.Rename(tmpFilePaths[i], zeroRPath); err != nil {
			coll.logger.Err("Failed to rename file from <" + tmpFilePaths[i] + "> to <" + zeroRPath + ">: " + err.Error())
			return
		}
	}
	for i := range filePaths { // remove files included in 0Rewrite
		if err := os.Remove(filePaths[i]); err != nil {
			coll.logger.Err("Failed to remove file <" + filePaths[i] + ">, error: " + err.Error())
		}
	}
}

// Will look into the instance folder and return the paths to each file inside it; and return the streamlined instance it read from all the files
func (coll *OfflineCollector) getFilePathsAndInstance() (filePaths []string, instance map[string]*OfflineCacheEntity, skip bool, err error) {
	coll.fileMux.Lock()
	defer coll.fileMux.Unlock()
	// Walk the directory to collect file paths
	if err := filepath.WalkDir(coll.instanceFldrPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() { // Exclude root path from filePaths
			filePaths = append(filePaths, path)
		}
		return nil
	}); err != nil {
		return nil, nil, false, fmt.Errorf("error <%w> walking path <%v>", err, coll.instanceFldrPath)
	}

	if coll.shouldSkipRewrite(filePaths, coll.instanceFldrPath) {
		return nil, nil, true, nil
	}
	instance = make(map[string]*OfflineCacheEntity) // momentarily hold only necessary entities of all files of caching instance. Needed so we don’t write something which will be removed on the next coming files.
	for i := range filePaths {
		if err := readAndDecodeFile(filePaths[i], instance); err != nil {
			return nil, nil, false, fmt.Errorf("error <%w> reading file <%v>", err, filePaths[i])
		}
	}
	return
}

// decides weather to skip a rewrite or not
func (coll *OfflineCollector) shouldSkipRewrite(filePaths []string, instanceFldrPath string) bool {
	fileStat, _ := coll.file.Stat() // Get stat of dump file in current use
	var nonRewriteFiles int
	for _, fileName := range filePaths { // rewrite if more than 1 non "0Rewrite" file is found
		if !strings.HasPrefix(fileName, path.Join(instanceFldrPath, rewriteFileName)) {
			nonRewriteFiles++
			if nonRewriteFiles == 2 { // rewrite if new dump file is populated
				break
			}
		}
	}
	// there will always be at least 1 non rewriten file when engine is open
	if nonRewriteFiles == 1 && fileStat.Size() == 0 { // dont rewrite if dump file isnt populated
		return true
	}
	// Close current open dump file so that we can rewrite it
	if err := coll.file.Close(); err != nil {
		coll.logger.Err("error closing file <" + coll.file.Name() + ">: " + err.Error())
		return true // dont rewrite if errored
	}
	// Open a new file where the normal writing will continue
	if err := coll.populateEncoder(); err != nil {
		coll.logger.Err(err.Error())
		return true // dont rewrite if errored
	}
	return false
}
