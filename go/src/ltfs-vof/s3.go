package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"io"
	"io/ioutil"
	"log"
	"os"
	"time"
)

/*
// s3 bucket
type S3Bucket struct {
	svc        *s3.Client
	BucketName string
	Region     string
}

func NewBucket(bucketName string, region string) *S3Bucket {

	// delete the bucket if it exists
	s := S3Bucket{
		BucketName: bucketName,
		Region:     region,
	}
	// s.svc = NewS3Client({Region: s.Region})
	s.svc = s3.New(s3.Options{
		Region: s.Region,
	})
	if s.svc == nil {
		log.Fatal("S3Bucket.New: ", "unable to create s3 session")
	}

	// if already exists then return
	if s.doesExist() {
		return &s
	}

	// create the corresponding s3 bucket
	bucketInput := &s3.CreateBucketInput{
		Bucket: aws.String(s.BucketName),
	}
	// create bucket, print error
	_, err := s.svc.CreateBucket(bucketInput)
	if err != nil {
		log.Fatal("S3Bucket.Create: ", err.Error())
	}
	// set the bucket versioning on
	versioningInput := &s3.PutBucketVersioningInput{
		Bucket: aws.String(s.BucketName),
		VersioningConfiguration: &s3.VersioningConfiguration{
			Status: aws.String("Enabled"),
		},
	}
	// create bucket, print error
	_, err = s.svc.PutBucketVersioning(versioningInput)
	if err != nil {
		log.Fatal("S3Bucket.Versionsing: ", err.Error())
	}
	return &s
}
func (s *S3Bucket) Bucket() string {
	return s.BucketName
}

// List all versions and delete them including delete markers
func (s *S3Bucket) Cleanout() {
	// loop until no versions available in bucket
	keyMarker := ""
	for {
		params := &s3.ListObjectVersionsInput{
			Bucket:    aws.String(s.BucketName),
			KeyMarker: aws.String(keyMarker),
			MaxKeys:   aws.Int64(1000),
		}
		resp, err := s.svc.ListObjectVersions(params)
		if err != nil {
			log.Fatal("S3Bucket.Delete: ", err.Error())
		}
		// now delete each version
		for _, version := range resp.Versions {
			s.DeleteVersion(*version.VersionId, *version.Key)
		}
		// delete each delete marker
		for _, deleteMarker := range resp.DeleteMarkers {
			s.DeleteVersion(*deleteMarker.VersionId, *deleteMarker.Key)
		}
		keyMarker = *resp.KeyMarker
		if keyMarker == "" {
			break
		}
	}
}

// returns false if bucket doesn't exist or don't have permissions
func (s *S3Bucket) doesExist() bool {
	// create the corresponding s3 bucket
	params := &s3.HeadBucketInput{
		Bucket: aws.String(s.BucketName),
	}
	// Head Bucket will return no nil if either doesn't exist or
	_, err := s.svc.HeadBucket(params)
	if err != nil {
		return false
	}
	return true
}
func (s *S3Bucket) DeleteVersion(versionID, key string) {

	params := &s3.DeleteObjectInput{
		Bucket:    aws.String(s.BucketName),
		Key:       aws.String(key),
		VersionId: aws.String(versionID),
	}
	_, err := s.svc.DeleteObject(params)

	if err != nil {
		log.Fatal("DeleteVersion: ", err.Error())
	}
}
func (s *S3Bucket) CompareBuckets(target *S3Bucket) bool {

	// get a sorted map of versions and delete markers from the source bucket
	sourceVersions, sourceDeleteMarkers := s.ListVersions()
	// get a sorted map of versions and delete markers from the results bucket
	resultVersions, resultDeleteMarkers := target.ListVersions()

	// check that the version maps are identical

	var keyFailure bool
	if len(sourceVersions) != len(resultVersions) {
		keyFailure = true
	}
	for key, sourceVersion := range sourceVersions {
		if _, ok := resultVersions[key]; !ok {
			keyFailure = true
			continue
		}
		// loop through the versions and compare the versionId and Etag
		if len(sourceVersion) != len(resultVersions[key]) {
			keyFailure = true
			continue
		}
		for i, version := range sourceVersion {
			// check key
			if *version.Key != *resultVersions[key][i].Key {
				keyFailure = true
			}
			// check etag (MD5)
			if *version.ETag != *resultVersions[key][i].ETag {
				keyFailure = true
			}
			// check latest version
			if *version.IsLatest != *resultVersions[key][i].IsLatest {
				keyFailure = true
			}
		}
	}
	var deleteMarkerFailure bool
	if len(sourceDeleteMarkers) != len(resultDeleteMarkers) {
		deleteMarkerFailure = true
	}
	// check that all delete markers are identical
	for key, sourceMarker := range sourceDeleteMarkers {
		if _, ok := resultDeleteMarkers[key]; !ok {
			deleteMarkerFailure = true
			continue
		}
		// loop through the versions and compare the versionId and Etag
		for i, source := range sourceMarker {
			// check key
			if *source.Key != *resultDeleteMarkers[key][i].Key {
				deleteMarkerFailure = true
			}
			// check latest version
			if *source.IsLatest != *resultDeleteMarkers[key][i].IsLatest {
				deleteMarkerFailure = true
			}
		}
	}
	if keyFailure || deleteMarkerFailure {
		return false
	}
	return true
}

// returns a map of key to a slice of versions and delete markers
// the slices are sorted by the last modified date
func (s *S3Bucket) ListVersions() (map[string][]*s3.ObjectVersion, map[string][]*s3.DeleteMarkerEntry) {

	versions := make(map[string][]*s3.ObjectVersion, 0)
	deleteMarkers := make(map[string][]*s3.DeleteMarkerEntry, 0)

	// loop until no versions available in bucket
	keyMarker := ""
	for {
		params := &s3.ListObjectVersionsInput{
			Bucket:    aws.String(s.BucketName),
			KeyMarker: aws.String(keyMarker),
			MaxKeys:   aws.Int64(1000),
		}
		resp, err := s.svc.ListObjectVersions(params)
		if err != nil {
			log.Fatal("S3Bucket.ListVersions: ", err.Error())
		}
		// add each version to the map
		for _, version := range resp.Versions {
			key := *version.Key
			if _, ok := versions[key]; !ok {
				versions[key] = make([]*s3.ObjectVersion, 0)
			}
			versions[key] = append(versions[key], version)
		}
		// add each delete marker to the map
		for _, deleteMarker := range resp.DeleteMarkers {
			key := *deleteMarker.Key
			if _, ok := deleteMarkers[key]; !ok {
				deleteMarkers[key] = make([]*s3.DeleteMarkerEntry, 0)
			}
			deleteMarkers[key] = append(deleteMarkers[key], deleteMarker)
		}
		keyMarker = *resp.KeyMarker
		if keyMarker == "" {
			break
		}
	}
	return versions, deleteMarkers
}
*/

// THE REST ARE USED BY DECODER TO WRITE TO S3 TARGET BUCKET
func Put(bucket, key, region string, block string) {

	bucket = bucket + "-test"

	client := getClient(region)
	// open the block file
	f, err := os.Open(block)
	if err != nil {
		log.Fatal("Unable to open block for single block  upload: ", err)
	}
	defer f.Close()
	// read all the bytes
	data, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatal("Unable to read file for single block upload: ", err)
	}
	// create a reader
	r := io.ReadSeeker(bytes.NewReader(data))

	// create the corresponding
	params := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   r,
	}

	// put the object
	resp, err := client.PutObject(context.TODO(), params)
	if err != nil {
		log.Fatal("S3 PUT: ", err.Error())
	}
	fmt.Println(resp)
}

func DeleteMarker(bucket, key, region string) {

	bucket = bucket + "-test"
	client := getClient(region)
	params := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	_, err := client.DeleteObject(context.TODO(), params)
	if err != nil {
		log.Fatal("DeleteObject: ", err.Error())
	}
	// need to sleep so that delete marker is created before next version
	time.Sleep(1 * time.Second)
}
func DeleteVersion(versionID, bucket, key, region string) {
	bucket = bucket + "-test"

	client := getClient(region)
	params := &s3.DeleteObjectInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(key),
		VersionId: aws.String(versionID),
	}
	_, err := client.DeleteObject(context.TODO(), params)

	if err != nil {
		log.Fatal("DeleteVersion: ", err.Error())
	}
	// need to sleep so that delete marker is created before next version
	time.Sleep(1 * time.Second)
}

// put using multipart where each block is
func PutMultipart(bucket, key, region string, blocks []string) {
	bucket = bucket + "-test"

	client := getClient(region)

	// input for starting a multipart upload
	input := s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	//send command to start copy and get the upload id as it is needed later
	createOutput, err := client.CreateMultipartUpload(context.TODO(), &input)

	// see if command failed
	if err != nil || createOutput == nil {
		log.Fatal("Unable to create multipart upload: ", err)
	}
	if createOutput.UploadId == nil {
		log.Fatal("No upload id found in start upload request")
	}
	// success, store the upload id
	uploadId := *createOutput.UploadId

	// loop through blocks files uploading each block
	partsInfo := make([]types.CompletedPart, 0)
	for partNum, block := range blocks {
		// open file
		f, err := os.Open(block)
		if err != nil {
			log.Fatal("Unable to open file for multipart upload: ", err)
		}
		defer f.Close()
		// read all the bytes
		data, err := ioutil.ReadAll(f)
		if err != nil {
			log.Fatal("Unable to read file for multipart upload: ", err)
		}
		// create a reader
		r := io.ReadSeeker(bytes.NewReader(data))
		partInput := s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			PartNumber: aws.Int32(int32(partNum)),
			UploadId:   aws.String(uploadId),
			Body:       r,
		}
		uploadResult, err := client.UploadPart(context.TODO(), &partInput)
		if err != nil || uploadResult == nil {
			log.Fatal("Error uploading part: ", partNum, err)
		}
		// save off partinfo for completed multipart upload
		partInfo := types.CompletedPart{
			ETag:       uploadResult.ETag,
			PartNumber: aws.Int32(int32(partNum)),
		}
		partsInfo = append(partsInfo, partInfo)
	}

	// compelete the upload
	mpu := types.CompletedMultipartUpload{
		Parts: partsInfo,
	}

	complete := s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String(key),
		UploadId:        aws.String(uploadId),
		MultipartUpload: &mpu,
	}
	compOutput, err := client.CompleteMultipartUpload(context.TODO(), &complete)
	if err != nil || compOutput == nil {
		log.Fatal("Unable to complete multipart upload: ", err)
	}
}
func getClient(region string) *s3.Client {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal("unable to create s3 session")
	}
	return s3.NewFromConfig(cfg, func(options *s3.Options) {
		options.Region = region
	})
}
