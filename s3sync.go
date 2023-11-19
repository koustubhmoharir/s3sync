package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"mime"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/koustubhmoharir/pathern/patherngo/pathern"
	"github.com/koustubhmoharir/xtn/xtngo/xtn"
	log "github.com/sirupsen/logrus"
)

func main() {

	initPtr := flag.Bool("init", false, "initialize a directory for syncing")

	flag.Parse()

	s := syncer{}

	dirPath := flag.Arg(0)
	if !filepath.IsAbs(dirPath) {
		dirPath, _ = filepath.Abs(dirPath)
	}

	dirInfo, err := os.Stat(dirPath)
	if errors.Is(err, fs.ErrNotExist) {
		fmt.Println("The specified directory does not exist. Error details:")
		fmt.Println(err)
		os.Exit(1)
	}
	s.dir = dirPath
	s.dirMode = dirInfo.Mode()
	s.rootDir = s.dir
	for {
		syncPath := filepath.Join(s.rootDir, "s3sync.xtn")
		_, err := os.Stat(syncPath)
		if errors.Is(err, fs.ErrNotExist) {
			parentDir := filepath.Dir(s.rootDir)
			if parentDir == s.rootDir {
				if *initPtr {
					s.rootDir = s.dir
					s.init()
					return
				}
				fmt.Println("s3sync.xtn was not found in the specified directory or any of its parents.")
				os.Exit(1)
			}
			s.rootDir = parentDir
			continue
		}
		break
	}

	if *initPtr {
		if s.rootDir == s.dir {
			s.init()
			return
		}
		fmt.Printf("s3sync.xtn was found in %v\nCannot init in a sub-directory of a directory that is already configured to sync. Modify this s3sync.xtn file instead to add sub-directories and patterns", s.rootDir)
		os.Exit(1)
	}

	configPath := filepath.Join(s.rootDir, ".s3sync", "config.xtn")
	_, err = os.Stat(configPath)
	if errors.Is(err, fs.ErrNotExist) {
		fmt.Printf("%v does not exist. Error details:", configPath)
		fmt.Println(err)
		os.Exit(1)
	}

	s.relDir, err = filepath.Rel(s.rootDir, s.dir)
	if err != nil {
		log.Fatalf("failed to get relative path from root directory, %v", err)
	}

	s.metaDir = filepath.Join(s.rootDir, ".s3sync", s.relDir)
	err = os.MkdirAll(s.metaDir, s.dirMode)
	if err != nil {
		fmt.Printf("Failed to create directory %v: %v\n", s.metaDir, err)
		os.Exit(1)
	}

	logFile, err := os.Create(filepath.Join(s.metaDir, "log.txt"))
	if err != nil {
		log.Fatalf("Failed to create log.txt: %v", err)
	}
	defer logFile.Close()

	muliLog := io.MultiWriter(os.Stdout, logFile)

	log.SetOutput(muliLog)

	s.loadConfig()

	s.findStatus()
	s.findIndex()
	if s.foundIndex && !s.foundStatus {
		log.Fatalf("No status.json was found in the bucket. The bucket_name may be wrong")
	}
	if s.index.Files == nil {
		s.index.Files = make(map[string]*IndexFileRecord)
	}
	s.scanDirectory(s.dir, "")
	if !s.foundStatus {
		log.Info("status.json was not found in the bucket. Local files will be uploaded to the bucket and a status.json file will be created.")
		// TODO: Should check status of existing files before uploading
		s.remoteStatus = make(map[string]any)
		s.uploadAndDeleteFilesOnRemote()
		s.uploadChangesFile(1)
		s.uploadStatusFile(1)
		s.createIndexFile(1)
		return
	}
	// s.foundStatus must be true at this point
	remoteVersion := uint64(s.remoteStatus["Version"].(float64))
	if !s.foundIndex {
		if len(s.index.Files) > 0 {
			log.Fatalf("No index file was found. Files from s3 can be downloaded but the directory %s must be empty", s.dir)
		}
		log.Info("index.json was not found locally. Files from s3 will be downloaded and the index will be created.")
		s.downloadAllFiles()
		s.createIndexFile(remoteVersion)
		log.Infof("The sync has completed with %v errors.", s.errorCount)
		return
	}
	if remoteVersion == s.index.RemoteVersion {
		log.Info("There are no changes in the bucket.")
		s.uploadAndDeleteFilesOnRemote()
		if len(s.remoteActions) == 0 {
			log.Info("There are no local changes.")
			log.Infof("The sync has completed with %v errors.", s.errorCount)
			return
		}
		s.uploadChangesFile(remoteVersion + 1)
		s.uploadStatusFile(remoteVersion + 1)
		s.createIndexFile(remoteVersion + 1)
		log.Infof("The sync has completed with %v errors.", s.errorCount)
		return
	}
	if remoteVersion < s.index.RemoteVersion {
		log.Fatalf("Version in s3 (%v) is behind local version (%v). This is not supposed to happen", remoteVersion, s.index.RemoteVersion)
	}
	log.Info("There are changes in the bucket. These changes will be applied to the local files.")
	success := s.applyRemoteChanges(remoteVersion)
	if success {
		s.uploadAndDeleteFilesOnRemote()
		if len(s.remoteActions) > 0 {
			remoteVersion = remoteVersion + 1
			s.uploadChangesFile(remoteVersion)
			s.uploadStatusFile(remoteVersion)
		}
		s.createIndexFile(remoteVersion)
		log.Infof("The sync has completed with %v errors.", s.errorCount)
	} else {
		s.createIndexFile(s.index.RemoteVersion)
		log.Infof("The sync is incomplete with %v conflicts and %v errors. Run the sync again after resolving conflicts and deleting the .conflict files.", s.conflictCount, s.errorCount)
	}
}

func (s *syncer) init() {
	metaDir := filepath.Join(s.rootDir, ".s3sync")
	err := os.MkdirAll(metaDir, s.dirMode)
	if err != nil {
		fmt.Printf("Failed to create directory %v: %v\n", metaDir, err)
		os.Exit(1)
	}

	syncPath := filepath.Join(s.rootDir, "s3sync.xtn")
	_, err = os.Stat(syncPath)
	if errors.Is(err, fs.ErrNotExist) {
		syncFile, err := os.Create(syncPath)
		if err != nil {
			fmt.Printf("Failed to create file %v: %v\n", syncPath, err)
			os.Exit(1)
		}
		defer syncFile.Close()
		_, err = fmt.Fprintf(syncFile, `

# The name of the Amazon S3 bucket to be used for syncing.
bucket_name: 

# A key prefix that can be used to restrict syncing to only a subset of files in the bucket.
key_prefix: 


.{}:
	# Add patterns for files that should be synced with the bucket
	<patterns>[]:
		+: **/*
	----
	# Add objects with the same syntax as this object for any child directories that should be synced independently of this directory. The key of the object should be the directory name.
	# The structure can be nested arbitrarily deeply. <patterns> should be omitted for directories that should not sync independently.
	# The patterns at a higher level in the hierarchy should not include any files from directories that will be synced independently. Hence if any objects are added below, the <patterns> above should be modified to exclude files that are targeted at a sub-directory level.
----
	`)
		if err != nil {
			fmt.Printf("Failed to write to the file %v: %v\n", syncPath, err)
			os.Exit(1)
		}
	}

	configPath := filepath.Join(metaDir, "config.xtn")
	_, err = os.Stat(configPath)
	if errors.Is(err, fs.ErrNotExist) {
		configFile, err := os.Create(configPath)
		if err != nil {
			fmt.Printf("Failed to create file %v: %v\n", configPath, err)
			os.Exit(1)
		}
		defer configFile.Close()

		_, err = fmt.Fprintf(configFile, `
# The name of the aws profile to be used for region selection and credentials. Refer to the AWS documentation on configuring a profile.
aws_profile: 

# Any unique name to distinguish this particular folder from any other folders that will also be synced against the same bucket.
source_name: 
`)
		if err != nil {
			fmt.Printf("Failed to write to the file %v: %v\n", configPath, err)
			os.Exit(1)
		}
	}

	fmt.Printf("Fill in the config files created at %v and %v\n", configPath, syncPath)
}

type syncer struct {
	rootDir       string
	dir           string
	relDir        string
	metaDir       string
	dirMode       fs.FileMode
	patterns      []pathern.PathPattern
	s3            *s3.Client
	bucketName    string
	keyPrefix     string
	metaKeyPrefix string
	sourceName    string
	remoteStatus  map[string]any
	foundStatus   bool
	index         LocalIndex
	foundIndex    bool
	remoteActions map[string]RemoteFileRecord
	errorCount    int64
	conflictCount int64
}

type RemoteStatus struct {
	Version uint64
}

type ChangeLog struct {
	Changes map[string]RemoteFileRecord
}

type LocalIndex struct {
	RemoteVersion uint64
	Files         map[string]*IndexFileRecord
}

type IndexFileRecord struct {
	Time              int64
	HashSha256        string
	RemoteEtag        string
	CurrentTime       int64  `json:"-"`
	CurrentHashSha256 string `json:"-"`
	Action            string `json:"-"`
}

type RemoteFileRecord struct {
	Action string
	Etag   string
}

func (s *syncer) loadConfig() {
	configPath := filepath.Join(s.rootDir, ".s3sync", "config.xtn")
	syncPath := filepath.Join(s.rootDir, "s3sync.xtn")

	configBytes, err := os.ReadFile(configPath)
	if err != nil {
		log.Fatal(err)
		return
	}

	syncBytes, err := os.ReadFile(syncPath)
	if err != nil {
		log.Fatal(err)
		return
	}

	var localConfig map[string]any
	xtn.UnmarshalToMap(configBytes, &localConfig)

	var syncConfig map[string]any
	xtn.UnmarshalToMap(syncBytes, &syncConfig)

	awsProfile := readConfigString(localConfig, "aws_profile", "key aws_profile must exist in config.xtn. The value can be left blank to use the default aws profile")

	s.bucketName = readConfigString(syncConfig, "bucket_name", "key bucket_name must exist in s3sync.xtn")
	if s.bucketName == "" {
		log.Fatal("the value of key bucket_name in s3sync.xtn must be the name of a s3 bucket. It cannot be blank")
	}

	keyPrefix := filepath.FromSlash(readConfigString(syncConfig, "key_prefix", "key key_prefix must exist in s3sync.xtn. The value can be left blank to sync the directory against the entire bucket"))
	if s.relDir != "." {
		s.keyPrefix = filepath.ToSlash(filepath.Join(keyPrefix, s.relDir))
	}
	s.metaKeyPrefix = filepath.ToSlash(filepath.Join(keyPrefix, ".s3sync", s.relDir))
	if len(s.keyPrefix) > 0 && !strings.HasSuffix(s.keyPrefix, "/") {
		s.keyPrefix = s.keyPrefix + "/"
	}
	if !strings.HasSuffix(s.metaKeyPrefix, "/") {
		s.metaKeyPrefix = s.metaKeyPrefix + "/"
	}

	s.sourceName = readConfigString(localConfig, "source_name", "key source_name must exist in config.xtn")
	if s.sourceName == "" {
		log.Fatal("the value of key source_name in config.xtn must be a unique name for a folder. It cannot be blank")
	}

	dirConfig := readConfigDir(syncConfig, s.relDir)
	patterns, ok := dirConfig["<patterns>"]
	if !ok {
		log.Fatalf("there are no patterns in s3sync.xtn for the specified directory")
	}
	switch patterns := patterns.(type) {
	case []any:
		for _, p := range patterns {
			switch p := p.(type) {
			case string:
				s.patterns = append(s.patterns, pathern.New(p))
			}
		}
	default:
		log.Fatalf("patterns must be an array in s3sync.xtn")
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithSharedConfigProfile(awsProfile))
	if err != nil {
		log.Fatalf("failed to load configuration, %v", err)
	}
	s.s3 = s3.NewFromConfig(cfg)
}

func (s *syncer) findStatus() {
	response, err := s.s3.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(s.metaKeyPrefix + "status.json"),
	})
	if err != nil {
		var re *awshttp.ResponseError
		if errors.As(err, &re) && re.Response.StatusCode == 404 {
			return
		} else {
			log.Fatal("Error downloading status.json from s3 bucket", err)
		}
	}
	defer response.Body.Close()

	dec := json.NewDecoder(response.Body)
	err = dec.Decode(&s.remoteStatus)
	if err != nil {
		log.Fatalf("Error reading status.json: %v", err)
	}
	_, ok := s.remoteStatus["Version"]
	if !ok {
		log.Fatalf("status.json in the s3 bucket does not contain a Version: %v", err)
	}
	s.foundStatus = true
}

func (s *syncer) findIndex() {
	indexPath := filepath.Join(s.metaDir, "index.json")
	_, err := os.Stat(indexPath)
	if errors.Is(err, fs.ErrNotExist) {
		return
	}
	indexBytes, err := os.ReadFile(indexPath)
	if err != nil {
		log.Fatalf("error reading index.json: %v", err)
	}

	err = json.Unmarshal(indexBytes, &s.index)
	if err != nil {
		log.Fatalf("error reading index.json: %v", err)
	}
	s.foundIndex = true
}

func (s *syncer) shouldSync(key string) bool {
	accept := false
	for _, p := range s.patterns {
		_, ok := p.Match(key)
		if ok {
			accept = true
		}
		// if pattern is negated and !ok, set accept to false
	}
	return accept
}

func (s *syncer) scanDirectory(dir string, prefix string) {
	file, err := os.Open(dir)
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	list, err := file.Readdir(-1)
	if err != nil {
		log.Fatal(err)
	}

	for _, f := range list {
		name := f.Name()
		if strings.HasPrefix(name, ".") {
			continue
		}
		if f.IsDir() {
			s.scanDirectory(filepath.Join(dir, name), prefix+name+"/")
		} else {
			key := prefix + name
			ext := filepath.Ext(key)
			extLessKey, _ := strings.CutSuffix(key, ext)
			if strings.HasSuffix(extLessKey, ".conflict.remote") || strings.HasSuffix(extLessKey, ".conflict.delete") {
				continue
			}
			if !s.shouldSync(key) {
				continue
			}
			record, ok := s.index.Files[key]
			if !ok {
				record = &IndexFileRecord{}
				s.index.Files[key] = record
			}
			record.CurrentTime = f.ModTime().UnixMilli()
			if record.CurrentTime != record.Time {
				record.CurrentHashSha256 = calculateHash(filepath.Join(dir, name))
			}
		}
	}
}

func calculateHash(path string) string {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		log.Fatal(err)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (s *syncer) uploadAndDeleteFilesOnRemote() {
	s.remoteActions = make(map[string]RemoteFileRecord)
	keysToDelete := make([]string, 0, 20)
	uploadCount := uint64(0)
	deleteCount := uint64(0)
	for key, record := range s.index.Files {
		if record.CurrentTime == record.Time {
			log.Debugf("Ignoring file %v because its last modified time has not changed", key)
			record.Action = "none"
			continue
		}
		if record.Action == "conflict" {
			log.Fatalf("File %v has been marked conflicted. We shouldn't be here in such a case", key)
			continue
		}
		if record.CurrentTime != 0 {
			path := filepath.Join(s.dir, filepath.FromSlash(key))
			ext := filepath.Ext(path)
			file, err := os.Open(path)
			if err != nil {
				s.errorCount++
				log.Errorf("Couldn't open file %v to upload. The file will be ignored: %v\n", path, err)
				record.Action = "error"
				continue
			}
			fullKey := s.keyPrefix + key
			response, err := s.s3.PutObject(context.TODO(), &s3.PutObjectInput{
				Bucket:      aws.String(s.bucketName),
				Key:         aws.String(fullKey),
				Body:        file,
				ContentType: aws.String(mime.TypeByExtension(ext)),
			})
			if err != nil {
				s.errorCount++
				log.Errorf("Could not upload file %v. Will proceed with other files: %v\n", path, err)
				record.Action = "error"
				continue
			}
			log.Debugf("Uploaded %v", key)
			uploadCount++
			record.Time = record.CurrentTime
			record.HashSha256 = record.CurrentHashSha256
			record.RemoteEtag = *response.ETag
			s.remoteActions[fullKey] = RemoteFileRecord{Action: "put", Etag: record.RemoteEtag}
			record.Action = "remote-put"
		} else {
			fullKey := s.keyPrefix + key
			_, err := s.s3.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
				Bucket: aws.String(s.bucketName),
				Key:    aws.String(fullKey),
			})
			if err != nil {
				s.errorCount++
				log.Errorf("Failed to delete %v from s3. Will proceed with other files: %v", fullKey, err)
				record.Action = "error"
				continue
			}
			deleteCount++
			log.Debugf("Deleted %v", key)
			keysToDelete = append(keysToDelete, key)
			s.remoteActions[fullKey] = RemoteFileRecord{Action: "delete"}
		}
	}
	for _, key := range keysToDelete {
		delete(s.index.Files, key)
	}
	log.Infof("Uploaded %v files to the s3 bucket and deleted %v files from the s3 bucket", uploadCount, deleteCount)
}

func (s *syncer) downloadAllFiles() {
	var continuationToken *string
	continuationToken = nil
	done := false
	count := uint64(0)
	for !done {
		response, err := s.s3.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucketName),
			Prefix:            aws.String(s.keyPrefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			log.Fatalf("Error listing files from s3: %v", err)
		}

		for _, obj := range response.Contents {
			relKey, _ := strings.CutPrefix(*obj.Key, s.keyPrefix)
			if strings.HasPrefix(relKey, ".") || strings.Contains(relKey, "/.") {
				log.Debugf("Ignoring key %v because one of its segments starts with a dot", relKey)
				continue
			}
			localPath, conflictPath := s.downloadSingleFile(*obj.Key, relKey)
			count++
			s.updateIndex(localPath, conflictPath, *obj.ETag)
		}

		continuationToken = response.NextContinuationToken
		if !response.IsTruncated {
			done = true
		}
	}
	log.Infof("%v files downloaded from s3 bucket", count)
}

func (s *syncer) applyRemoteChanges(remoteVersion uint64) bool {
	allChanges := ChangeLog{Changes: make(map[string]RemoteFileRecord)}
	for v := s.index.RemoteVersion + 1; v <= remoteVersion; v++ {
		changes := s.readChangesFile(v)
		for fullKey, record := range changes.Changes {
			allChanges.Changes[fullKey] = record
		}
	}
	// When we reach here for the first time
	//   if a file has been added / modified remotely
	//     if the file has not been modified locally, download the remote file and update the index
	//     if the file has been modified locally or been deleted, create a .conflict.remote file, and set RemoteEtag
	//   if a file has been deleted remotely
	//     if the file has not been modified locally, delete the file and set RemoteEtag to empty
	//     if the file has been deleted locally, just set RemoteEtag to empty
	//     if the file has been modified locally, create a .conflict.delete file, and set RemoteEtag to empty
	//   if there are any conflicts, no files are uploaded or deleted on the remote, but the changes to the index are saved
	//   the user must resolve conflicts manually by comparing the .conflict files with the local files
	//   local files are changed by the user as appropriate
	//   the conflict is marked as resolved by deleting the .conflict files
	// When we reach here after manual conflict resolution
	//   if the RemoteEtag in the index matches the remote etag
	//     if the .conflict file has been deleted, changes on remote are ignored because they have already been seen
	//     else the conflict files are retained
	//   if the RemoteEtag does not match, there are further remote changes, so the conflict files are recreated
	downloadCount := uint64(0)
	deleteCount := uint64(0)
	for fullKey, remoteRecord := range allChanges.Changes {
		relKey, _ := strings.CutPrefix(fullKey, s.keyPrefix)
		record, ok := s.index.Files[relKey]
		if remoteRecord.Action == "put" {
			if ok && record.RemoteEtag == remoteRecord.Etag {
				if s.conflictFileExists(relKey, ".conflict.remote") {
					record.Action = "conflict"
					s.conflictCount++
				}
			} else {
				localPath, conflictPath := s.downloadSingleFile(fullKey, relKey)
				s.updateIndex(localPath, conflictPath, remoteRecord.Etag)
				if localPath != conflictPath {
					s.conflictCount++
				} else {
					downloadCount++
				}
			}
		} else if remoteRecord.Action == "delete" {
			if ok && record.RemoteEtag == "" {
				if s.conflictFileExists(relKey, ".conflict.delete") {
					record.Action = "conflict"
					s.conflictCount++
				}
			} else {
				deleted := s.deleteLocalFile(relKey)
				if !deleted {
					s.conflictCount++
				} else if ok && record.CurrentTime != 0 {
					deleteCount++
				}
			}
		}
	}
	log.Infof("%v files were downloaded from the bucket and %v files were deleted locally", downloadCount, deleteCount)
	if s.conflictCount > 0 {
		s.conflictCount++
		log.Errorf("There are %v conflicts. Look for files with .conflict.remote and .conflict.delete", s.conflictCount)
	}
	return s.conflictCount == 0
}

func (s *syncer) readChangesFile(v uint64) *ChangeLog {
	// TODO: Download and keep the changes file cached, and read from local if available
	changesName := fmt.Sprintf("changes_%v.json", v)
	changesPath := filepath.Join(s.metaDir, changesName)
	changesFile, err := os.Open(changesPath)
	if errors.Is(err, fs.ErrNotExist) {
		response, err := s.s3.GetObject(context.TODO(), &s3.GetObjectInput{
			Bucket: aws.String(s.bucketName),
			Key:    aws.String(s.metaKeyPrefix + changesName),
		})
		if err != nil {
			log.Fatalf("Error downloading file from s3 bucket: %v", err)
		}
		defer response.Body.Close()
		changesFile, err = os.Create(changesPath)
		if err != nil {
			log.Fatalf("Error creating changes file: %v", err)
		}
		defer changesFile.Close()
		io.Copy(changesFile, response.Body)
		changesFile.Seek(0, 0)
	} else if err != nil {
		log.Fatalf("Error reading changes file: %v", err)
	} else {
		defer changesFile.Close()
	}
	var changes ChangeLog
	dec := json.NewDecoder(changesFile)
	err = dec.Decode(&changes)
	if err != nil {
		log.Fatalf("Error reading %v: %v", changesName, err)
	}
	return &changes
}

func (s *syncer) downloadSingleFile(key string, relKey string) (string, string) {
	response, err := s.s3.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		log.Fatalf("Error downloading file %v from s3 bucket: %v", key, err)
	}
	defer response.Body.Close()

	outPath := filepath.Join(s.dir, filepath.FromSlash(relKey))
	outDirPath := filepath.Dir(outPath)
	err = os.MkdirAll(outDirPath, s.dirMode)
	if err != nil {
		log.Fatalf("Failed to create directory at %v: %v", outDirPath, err)
	}

	localPath := outPath
	record, ok := s.index.Files[relKey]
	if ok && record.CurrentTime != record.Time && record.CurrentHashSha256 != record.HashSha256 {
		ext := filepath.Ext(outPath)
		outPath, _ = strings.CutSuffix(outPath, ext)
		outPath = outPath + ".conflict.remote" + ext
	}

	outFile, err := os.Create(outPath)
	if err != nil {
		log.Fatalf("Failed to create file at %v: %v", outPath, err)
	}
	defer outFile.Close()
	_, err = io.Copy(outFile, response.Body)
	if err != nil {
		log.Fatalf("Failed to write file at %v: %v", outPath, err)
	}
	log.Debugf("Saved file for key %v at %v", relKey, outPath)
	return localPath, outPath
}

func (s *syncer) conflictFileExists(relKey string, conflictSuffix string) bool {
	ext := filepath.Ext(relKey)
	extLessKey, _ := strings.CutSuffix(relKey, ext)
	conflictPath := filepath.Join(s.dir, filepath.FromSlash(extLessKey+conflictSuffix+ext))
	_, err := os.Stat(conflictPath)
	return !errors.Is(err, fs.ErrNotExist)
}

func (s *syncer) deleteLocalFile(relKey string) bool {
	localPath := filepath.Join(s.dir, filepath.FromSlash(relKey))
	outPath := localPath
	record, ok := s.index.Files[relKey]
	if ok && record.CurrentTime != 0 && record.CurrentTime != record.Time && record.CurrentHashSha256 != record.HashSha256 {
		ext := filepath.Ext(outPath)
		outPath, _ = strings.CutSuffix(outPath, ext)
		outPath = outPath + ".conflict.delete" + ext
		outFile, err := os.Create(outPath)
		if err != nil {
			log.Fatalf("Failed to create a file at %v: %v", outPath, err)
		}
		outFile.Close()
		record.RemoteEtag = ""
		record.Action = "conflict"
		return false
	} else {
		delete(s.index.Files, relKey)
		os.Remove(localPath)
		return true
	}
}

func (s *syncer) updateIndex(path string, conflictPath string, etag string) {
	relKey, _ := strings.CutPrefix(path, s.dir)
	relKey = filepath.ToSlash(relKey)
	relKey, _ = strings.CutPrefix(relKey, "/")

	record, ok := s.index.Files[relKey]
	if !ok {
		record = &IndexFileRecord{}
		s.index.Files[relKey] = record
	}
	record.RemoteEtag = etag
	if path == conflictPath {
		info, err := os.Stat(path)
		if err != nil {
			log.Fatalf("Error retrieving information about %v: %v", path, err)
		}
		record.Time = info.ModTime().UnixMilli()
		record.CurrentTime = record.Time
		record.HashSha256 = calculateHash(path)
		record.CurrentHashSha256 = ""
		record.Action = "local-put"
	} else {
		record.Action = "conflict"
	}
}

func (s *syncer) uploadChangesFile(newVersion uint64) {
	changesName := fmt.Sprintf("changes_%v.json", newVersion)
	changesPath := filepath.Join(s.metaDir, changesName)
	changesFile, err := os.Create(changesPath)
	if err != nil {
		log.Fatalf("Failed to create changes file: %v", err)
	}
	defer changesFile.Close()
	encoder := json.NewEncoder(changesFile)
	changesObj := make(map[string]any)
	changesObj["Changes"] = s.remoteActions
	encoder.Encode(changesObj)
	changesFile.Seek(0, 0)

	_, err = s.s3.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(s.metaKeyPrefix + changesName),
		Body:   changesFile,
	})
	if err != nil {
		log.Fatalf("Could not upload file %v: %v\n", changesPath, err)
	}
	log.Infof("%v was uploaded", changesName)
}

func (s *syncer) uploadStatusFile(newVersion uint64) {
	s.remoteStatus["Version"] = newVersion
	statusJson, err := json.Marshal(s.remoteStatus)
	if err != nil {
		log.Fatalf("Failed to create status json: %v", err)
	}

	_, err = s.s3.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(s.metaKeyPrefix + "status.json"),
		Body:   bytes.NewReader(statusJson),
	})
	if err != nil {
		log.Fatalf("Could not upload status.json: %v", err)
	}
	log.Infof("status.json file was uploaded with version %v", newVersion)
}

func (s *syncer) createIndexFile(newVersion uint64) {
	s.index.RemoteVersion = newVersion
	// TODO: remove keys with an action of local-delete
	indexPath := filepath.Join(s.metaDir, "index.json")
	indexFile, err := os.Create(indexPath)
	if err != nil {
		log.Fatalf("Failed to create index file: %v", err)
	}
	defer indexFile.Close()
	encoder := json.NewEncoder(indexFile)
	encoder.Encode(s.index)
	log.Info("index.json file was saved")
}

func readConfigString(config map[string]any, key string, missingMsg string) string {
	v, ok := config[key]
	if !ok {
		log.Fatal(missingMsg)
	}
	s, ok := v.(string)
	if !ok {
		log.Fatalf("key %s in s3sync.xtn must be text\n", key)
	}
	return s
}

func readConfigDir(config map[string]any, relPath string) map[string]any {
	dir, last := filepath.Split(relPath)
	dir = filepath.Clean(dir)
	if last != "." {
		config = readConfigDir(config, dir)
	}
	res, ok := config[last]
	if !ok {
		log.Fatalf("s3sync.xtn does not have any configuration key for the %v directory.", last)
	}
	switch res := res.(type) {
	case map[string]any:
		return res
	default:
		log.Fatalf("s3sync.xtn has invalid configuration for the %v directory. The value must be an object", last)
	}
	return nil
}
