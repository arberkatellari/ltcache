/*
TransCache is released under the MIT License <http://www.opensource.org/licenses/mit-license.php
Copyright (C) ITsysCOM GmbH. All Rights Reserved.
*/

package ltcache

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"log"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestEnsureDir(t *testing.T) {
	path := "/tmp/testEnsureDir"
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)
	if err := ensureDir(path); err != nil {
		t.Error(err)
	}
}

func TestEnsureDirCreation(t *testing.T) {
	path := "/tmp/testEnsureDir"
	if err := ensureDir(path); err != nil {
		t.Error(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Error(err)
	}
	if err := os.RemoveAll(path); err != nil {
		t.Error(err)
	}
}

func TestEnsureDirErr(t *testing.T) {
	path := "/root/testEnsureDir"
	expErr := "stat /root/testEnsureDir: permission denied"
	if err := ensureDir(path); err == nil || err.Error() != expErr {
		t.Errorf("Expected error <%v>, Received <%v>", expErr, err)
	}
}

func TestPopulateEncodersErr(t *testing.T) {
	expErr := "no such file or directory"
	if _, _, _, err := populateEncoder("/tmp/testOff/*default"); err == nil ||
		!strings.Contains(err.Error(), expErr) {
		t.Errorf("Expected error <%v>, Received <%v>", expErr, err)
	}
}

func TestOfflineCollectorWriteEntityErr(t *testing.T) {
	path := "/tmp/*default"
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path+"/file", os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644)
	if err != nil {
		t.Error(err)
	}
	var logBuf bytes.Buffer
	writer := bufio.NewWriter(f)
	defer os.RemoveAll(path)
	oc := &OfflineCollector{
		writer:  writer,
		encoder: gob.NewEncoder(writer),
		logger:  &testLogger{log.New(&logBuf, "", 0)},
	}
	f.Close()
	oce := OfflineCacheEntity{
		IsSet:    true,
		ItemID:   "item1",
		Value:    "val1",
		GroupIDs: []string{"gr1"},
	}
	expErr := "write error: <write /tmp/*default/file: file already closed>"
	expBuf := `Error <write error: <write /tmp/*default/file: file already closed>>, writing cache item <ltcache.OfflineCacheEntity{IsSet:true, ItemID:"item1", Value:"val1", GroupIDs:[]string{"gr1"}, ExpiryTime:time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC)}`
	if err := oc.writeEntity(oce); err == nil || expErr != err.Error() {
		t.Errorf("expected error <%v>, received <%v>", expErr, err)
	} else if rcv := logBuf.String(); !strings.Contains(rcv, expBuf) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expBuf, rcv)
	}
}

func TestValidateFilePathsOldRewriteName(t *testing.T) {
	path := "/tmp/*default"
	if err := os.MkdirAll(path+"/0Rewrite", 0755); err != nil {
		t.Fatal(err)
	}
	if validPaths, err := validateFilePaths([]string{path + "/oldRewrite",
		path + "/0Rewrite"}, path); err != nil {
		t.Error(err)
	} else if !slices.Contains(validPaths, "/tmp/*default/oldRewrite") {
		t.Errorf("Expected <%+v>, Received <%+v>",
			[]string{"/tmp/*default/oldRewrite"}, validPaths)
	}
	if _, err := os.Stat(path + "/0Rewrite"); !os.IsNotExist(err) {
		t.Errorf("Expected folder to be deleted, received error <%v>", err)
	}
}
func TestValidateFilePathsRewriteFileErr(t *testing.T) {
	path := "/tmp/*default"
	expErr := "remove /tmp/*default/0Rewrite: no such file or directory"
	if _, err := validateFilePaths([]string{path + "/oldRewrite",
		path + "/0Rewrite"}, path); err == nil || err.Error() != expErr {
		t.Errorf("Expected error <%v>, Received <%v>", expErr, err)
	}
}

func TestValidateFilePathsTmpRewrite(t *testing.T) {
	path := "/tmp/*default"
	if err := os.MkdirAll(path+"/tmpRewrite", 0755); err != nil {
		t.Fatal(err)
	}
	expErr := "remove /tmp/*default/tmpRewrite: no such file or directory"
	if validPaths, err := validateFilePaths([]string{path +
		"/tmpRewrite"}, path); err != nil {
		t.Errorf("Expected error <%v>, Received <%v>", expErr, err)
	} else if len(validPaths) != 0 {
		t.Errorf("Expected <%+v>, Received <%+v>", []string{}, validPaths)
	}
	if _, err := os.Stat(path + "/tmpRewrite"); !os.IsNotExist(err) {
		t.Errorf("Expected folder to be deleted, received error <%v>", err)
	}
}

func TestValidateFilePathsTmpRewriteErr(t *testing.T) {
	path := "/tmp/*default"
	expErr := "remove /tmp/*default/tmpRewrite: no such file or directory"
	if _, err := validateFilePaths([]string{path + "/tmpRewrite"}, path); err == nil ||
		err.Error() != expErr {
		t.Errorf("Expected error <%v>, Received <%v>", expErr, err)
	}
}

func TestReadAndDecodeFileDecodeSet(t *testing.T) {
	path := "/tmp/*default"
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)
	if f, err := os.OpenFile(path+"/file",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err != nil {
		t.Error(err)
	} else {
		defer f.Close()
		enc := gob.NewEncoder(f)
		enc.Encode(OfflineCacheEntity{
			IsSet:    true,
			ItemID:   "testID",
			Value:    "value",
			GroupIDs: []string{"gpID"},
		})
	}
	oceMap := map[string]*OfflineCacheEntity{}
	handleEntity := func(oce *OfflineCacheEntity) { // will add/delete OfflineCacheEntity from oceMap
		if oce.IsSet {
			oceMap[oce.ItemID] = oce
		} else {
			delete(oceMap, oce.ItemID)
		}
	}
	exp := map[string]*OfflineCacheEntity{
		"testID": {IsSet: true, ItemID: "testID", Value: "value",
			GroupIDs: []string{"gpID"}},
	}
	if err := readAndDecodeFile(path+"/file", handleEntity); err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(exp, oceMap) {
		t.Errorf("Expected <%+v>, Received <%+v>", exp, oceMap)
	}
}

func TestReadAndDecodeFileDecodeRemove(t *testing.T) {
	path := "/tmp/*default"
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)
	if f, err := os.OpenFile(path+"/file",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err != nil {
		t.Error(err)
	} else {
		defer f.Close()
		enc := gob.NewEncoder(f)
		enc.Encode(OfflineCacheEntity{
			IsSet:    false,
			ItemID:   "testID",
			Value:    "value",
			GroupIDs: []string{"gpID"},
		})
	}
	oceMap := map[string]*OfflineCacheEntity{
		"testID": {
			IsSet:    false,
			ItemID:   "testID",
			Value:    "value",
			GroupIDs: []string{"gpID"},
		},
	}
	handleEntity := func(oce *OfflineCacheEntity) { // will add/delete OfflineCacheEntity from oceMap
		if oce.IsSet {
			oceMap[oce.ItemID] = oce
		} else {
			delete(oceMap, oce.ItemID)
		}
	}
	exp := map[string]*OfflineCacheEntity{}
	if err := readAndDecodeFile(path+"/file", handleEntity); err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(exp, oceMap) {
		t.Errorf("Expected <%+v>, Received <%+v>", exp, oceMap)
	}
}

func TestReadAndDecodeFileErr1(t *testing.T) {
	expErr := "error opening file <> in memory: open : no such file or directory"
	oceMap := map[string]*OfflineCacheEntity{}
	handleEntity := func(oce *OfflineCacheEntity) {
		if oce.IsSet {
			oceMap[oce.ItemID] = oce
		} else {
			delete(oceMap, oce.ItemID)
		}
	}
	if err := readAndDecodeFile("", handleEntity); err == nil ||
		err.Error() != expErr {
		t.Errorf("Expected error <%v>, Received <%v>", expErr, err)
	}
}

func TestOfflineCollectorCollect(t *testing.T) {
	oc := &OfflineCollector{
		collection: map[string]*CollectionEntity{},
	}
	expOC := &OfflineCollector{
		collection: map[string]*CollectionEntity{
			"CacheID1": {
				IsSet:  true,
				ItemID: "CacheID1",
			},
		},
	}
	oc.collect("CacheID1")
	if !reflect.DeepEqual(expOC.collection, oc.collection) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expOC.collection, oc.collection)
	}
}

type testLogger struct {
	*log.Logger
}

func (l *testLogger) Alert(msg string) error {
	l.Println(msg)
	return nil
}
func (l *testLogger) Close() error {
	return nil
}
func (l *testLogger) Crit(msg string) error {
	l.Println(msg)
	return nil
}
func (l *testLogger) Debug(msg string) error {
	l.Println(msg)
	return nil
}
func (l *testLogger) Emerg(msg string) error {
	l.Println(msg)
	return nil
}
func (l *testLogger) Err(msg string) error {
	l.Println(msg)
	return nil
}
func (l *testLogger) Notice(msg string) error {
	l.Println(msg)
	return nil
}
func (l *testLogger) Warning(msg string) error {
	l.Println(msg)
	return nil
}
func (l *testLogger) Info(msg string) error {
	l.Println(msg)
	return nil
}

func TestOfflineCollectorCheckAndRotateFile(t *testing.T) {
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
	tmpFile.WriteString("writing")
	if newf, w, e, err := rotateFileIfNeeded(path+"/*default", 0, tmpFile); err != nil {
		t.Error(err)
	} else if newf == nil {
		t.Errorf("expected new file, received nil")
	} else if w == nil {
		t.Errorf("expected new writer, received nil")
	} else if e == nil {
		t.Errorf("expected new encoder, received nil")
	}
}

func TestOfflineCollectorStoreRemoveEntityNoInterval(t *testing.T) {
	var encBuf bytes.Buffer
	oc := &OfflineCollector{
		writeLimit: -1,
		writer:     bufio.NewWriter(&bytes.Buffer{}),
		encoder:    gob.NewEncoder(&encBuf),
	}
	bufExpect := "OfflineCacheEntity"
	oc.storeRemoveEntity("CacheID1", -1)
	if rcv := encBuf.String(); !strings.Contains(rcv, bufExpect) {
		t.Errorf("Expected to contain <%+v>, \nReceived <%+v>", bufExpect, rcv)
	}
}

func TestOfflineCollectorStoreRemoveEntityErr1(t *testing.T) {
	var logBuf bytes.Buffer
	oc := &OfflineCollector{
		writeLimit: 1,
		logger:     &testLogger{log.New(&logBuf, "", 0)},
	}
	bufExpect := "error getting file stat: invalid argument"
	oc.storeRemoveEntity("CacheID1", -1)
	if rcv := logBuf.String(); !strings.Contains(rcv, bufExpect) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", bufExpect, rcv)
	}
}

func TestOfflineCollectorStoreRemoveEntityInterval(t *testing.T) {
	oc := &OfflineCollector{
		collection: map[string]*CollectionEntity{},
	}
	exp := &CollectionEntity{
		IsSet:  false,
		ItemID: "CacheID1",
	}
	oc.storeRemoveEntity("CacheID1", 1)
	if !reflect.DeepEqual(exp, oc.collection["CacheID1"]) {
		t.Errorf("Expected <%+v>, \nreceived <%+v>", exp, oc.collection["CacheID1"])
	}
}

func TestOCRewriteErr1(t *testing.T) {
	var logBuf bytes.Buffer
	oc := OfflineCollector{
		fldrPath: "/tmp/notexistent",
		logger:   &testLogger{log.New(&logBuf, "", 0)},
	}
	bufExpect := "error <lstat /tmp/notexistent: no such file or directory> walking path </tmp/notexistent>"
	oc.rewriteFiles()
	if rcv := logBuf.String(); !strings.Contains(rcv, bufExpect) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", bufExpect, rcv)
	}
}

func TestOfflineCollectorShouldSkipRewriteTrue(t *testing.T) {
	path := "/tmp/internal_db/*default"
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path+"/otherFile", os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644)
	if err != nil {
		t.Error(err)
	}
	defer func() {
		f.Close()
		if err := os.RemoveAll("/tmp/internal_db"); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	fName := f.Name()
	oc := OfflineCollector{
		writeLimit: 1,
		file:       f,
		fldrPath:   path,
	}
	if oc.shouldSkipRewrite([]string{oc.fldrPath + "/otherFile"}, "") &&
		fName != oc.file.Name() {
		t.Errorf("Expected file <%s>, received <%s>", fName, oc.file.Name())
	}
}

func TestOfflineCollectorShouldSkipRewriteFalse(t *testing.T) {
	path := "/tmp/internal_db/*default"
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path+"/oldOtherFile", os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644)
	if err != nil {
		t.Error(err)
	}
	f.Close()
	f, err = os.OpenFile(path+"/otherFile", os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644)
	if err != nil {
		t.Error(err)
	}
	defer func() {
		f.Close()
		if err := os.RemoveAll("/tmp/internal_db"); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	fName := f.Name()
	oc := &OfflineCollector{
		writeLimit: 1,
		file:       f,
		fldrPath:   path,
	}
	if oc.shouldSkipRewrite([]string{path + "/oldOtherFile", oc.fldrPath + "/otherFile"},
		"") {
		t.Errorf("Expected shouldSkipRewrite false, received true")
	} else if fName == oc.file.Name() {
		t.Errorf("Expected file <%s>, received <%s>", fName, oc.file.Name())
	}
}
