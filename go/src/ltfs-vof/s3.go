// provides s3 services for both source simulator and customer target buckets
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
	. "ltfs-vof/utils"
	"os"
	"time"
)

type S3Simulator struct {
	region string
	bucket string
	logger *Logger
}

// create a bucket for the simulator to write to as a source
func NewS3Simulator(region, bucket string, versioning bool, logger *Logger) *S3Simulator {
	// make the bucket with versioning or not
	createBucket(region, bucket, versioning, logger)

	return &S3Simulator{
		region: region,
		bucket: bucket,
		logger: logger,
	}
}

// put a object to the s3 souce bucket
func (s *S3Simulator) Put(objectName string, data []byte) {

	client := getClient(s.region, s.logger)
	// create a reader
	r := io.ReadSeeker(bytes.NewReader(data))

	// create the corresponding
	params := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(objectName),
		Body:   r,
	}

	// put the object
	_, err := client.PutObject(context.TODO(), params)
	if err != nil {
		s.logger.Fatal("S3 Source PUT: ", err.Error())
	}
}
func (s *S3Simulator) Delete(objectName string) {
	deleteObject(s.region, s.bucket, objectName, true, s.logger)
}

// S3Customer is used to put data to the customer s3 target bucket
type S3Customer struct {
	region     string
	directory  string
	logger     *Logger
	versioning bool
	simulation bool
	buckets    []string
}

// store parameters so that they don't need to be passed each time
func NewS3Customer(region, directory string, versioning, simulation bool, logger *Logger) *S3Customer {
	return &S3Customer{
		region:     region,
		directory:  directory,
		logger:     logger,
		versioning: versioning,
		simulation: simulation,
	}
}

// for the S3 target the data is passed as a list of block files
func (s *S3Customer) Put(bucketName, objectName string, blockFiles []string) {

	// check for zero blocks
	if len(blockFiles) == 0 {
		s.logger.Fatal("Zero blocks files sent to Put")
	}

	// create bucket if doesn't exist
	s.checkBucket(bucketName)

	// if not in simulation mode and has more then one block file
	// then multipart upload
	if !s.simulation && len(blockFiles) > 1 {
		s.putMultipart(bucketName, objectName, blockFiles)
		return
	}
	// sum data from blockfiles together
	var fullData []byte
	for _, blockFile := range blockFiles {

		// open the block file
		f, err := os.Open(s.directory + "/" + bucketName + "/" + blockFile)
		if err != nil {
			s.logger.Fatal("Unable to open block for single block: ", blockFile, "  upload: ", err)
		}
		defer f.Close()
		// read all the bytes
		data, err := ioutil.ReadAll(f)
		if err != nil {
			s.logger.Fatal("Unable to read file for single block upload: ", blockFile, err)
		}
		fullData = append(fullData, data...)
	}
	// create a reader
	r := io.ReadSeeker(bytes.NewReader(fullData))

	// create the corresponding request
	params := &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
		Body:   r,
	}

	// put the object
	client := getClient(s.region, s.logger)
	_, err := client.PutObject(context.TODO(), params)
	if err != nil {
		s.logger.Fatal("S3 PUT: ", err.Error())
	}
}
func (s *S3Customer) Delete(bucketName, objectName string) {
	deleteObject(s.region, bucketName, objectName, true, s.logger)
}

// checks to see if bucket has already been created and if not creates it
func (s *S3Customer) checkBucket(bucketName string) {
	// if bucket is on list then return
	for _, bucket := range s.buckets {
		if bucket == bucketName {
			return
		}
	}
	s.logger.Event("Bucket ", bucketName, " doesn't exist on list so 	creating it")
	// not on list put it there and create the bucket
	s.buckets = append(s.buckets, bucketName)

	// create the bucket
	createBucket(s.region, bucketName, s.versioning, s.logger)
}

// put using multipart where each block is a part
func (s *S3Customer) putMultipart(bucket, key string, blockFiles []string) {

	client := getClient(s.region, s.logger)

	// input for starting a multipart upload
	input := s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	//send command to start copy and get the upload id as it is needed later
	createOutput, err := client.CreateMultipartUpload(context.TODO(), &input)

	// see if command failed
	if err != nil || createOutput == nil {
		s.logger.Fatal("Unable to create multipart upload: ", err)
	}
	if createOutput.UploadId == nil {
		s.logger.Fatal("No upload id found in start upload request")
	}
	// success, store the upload id
	uploadId := *createOutput.UploadId

	// loop through blocks files uploading each block
	partsInfo := make([]types.CompletedPart, 0)
	for partNum, block := range blockFiles {
		// open file
		f, err := os.Open(s.directory + "/" + bucket + "/" + block)
		if err != nil {
			s.logger.Fatal("Unable to open file for multipart upload: ", err)
		}
		defer f.Close()
		// read all the bytes
		data, err := ioutil.ReadAll(f)
		if err != nil {
			s.logger.Fatal("Unable to read file for multipart upload: ", err)
		}
		// create a reader
		r := io.ReadSeeker(bytes.NewReader(data))
		partInput := s3.UploadPartInput{
			Bucket:     aws.String(bucket + "-test"),
			Key:        aws.String(key),
			PartNumber: aws.Int32(int32(partNum + 1)),
			UploadId:   aws.String(uploadId),
			Body:       r,
		}
		uploadResult, err := client.UploadPart(context.TODO(), &partInput)
		if err != nil || uploadResult == nil {
			s.logger.Fatal("Error uploading part: ", partNum, err)
		}
		// save off partinfo for completed multipart upload
		partInfo := types.CompletedPart{
			ETag:       uploadResult.ETag,
			PartNumber: aws.Int32(int32(partNum + 1)),
		}
		partsInfo = append(partsInfo, partInfo)
	}

	// compelete the upload
	mpu := types.CompletedMultipartUpload{
		Parts: partsInfo,
	}

	complete := s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(bucket + "-test"),
		Key:             aws.String(key),
		UploadId:        aws.String(uploadId),
		MultipartUpload: &mpu,
	}
	compOutput, err := client.CompleteMultipartUpload(context.TODO(), &complete)
	if err != nil || compOutput == nil {
		s.logger.Fatal("Unable to complete multipart upload: ", err)
	}
}

// compare simulation buckets to the customer target buckets
func (s3 *S3Customer) Compare() {
	for _, bucket := range s3.buckets {
		s3.logger.Event("Comparing bucket ", bucket)
		fmt.Println("Comparing bucket ", bucket)
	}
}

// these functions are used by both the S3 source simulator and the S3 customer target
func createBucket(region, bucketName string, versioning bool, logger *Logger) {

	// if bucket exist then clean it out and return
	if doesExist(region, bucketName, logger) {
		logger.Event("Bucket ", bucketName, " already exists, cleaning out")
		cleanout(region, bucketName, logger)
	} else {
		// create bucket
		logger.Event("Bucket ", bucketName, " doesn't exists creating it")
		bucketInput := &s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
		}
		// create bucket, print error
		client := getClient(region, logger)
		_, err := client.CreateBucket(context.TODO(), bucketInput)
		if err != nil {
			logger.Fatal("S3Bucket.Create: ", err.Error())
		}
	}
	// if versioning is set then set for the bucket
	if versioning {
		logger.Event("Bucket ", bucketName, " Turning on versioning")
		versioningInput := &s3.PutBucketVersioningInput{
			Bucket: aws.String(bucketName),
			VersioningConfiguration: &types.VersioningConfiguration{
				Status: types.BucketVersioningStatusEnabled,
			},
		}
		client := getClient(region, logger)
		_, err := client.PutBucketVersioning(context.TODO(), versioningInput)
		if err != nil {
			logger.Fatal("S3Bucket.Versionsing: ", err.Error())
		}
	}
}
func deleteObject(bucket, key, region string, sleep bool, logger *Logger) {

	logger.Event("Deleting object ", key, " from bucket ", bucket)
	client := getClient(region, logger)
	params := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	_, err := client.DeleteObject(context.TODO(), params)
	if err != nil {
		logger.Fatal("DeleteObject: ", err.Error())
	}
	// need to sleep so that delete marker is created before next version
	if sleep {
		time.Sleep(1 * time.Second)
	}
}

// List all versions and delete them including delete markers
func cleanout(region, bucketName string, logger *Logger) {
	// loop until no versions available in bucket
	keyMarker := ""
	for {
		params := &s3.ListObjectVersionsInput{
			Bucket:    aws.String(bucketName),
			KeyMarker: aws.String(keyMarker),
			MaxKeys:   aws.Int32(1000),
		}
		fmt.Println("Cleaning out bucket ", bucketName, " Be patient this can take a while")
		client := getClient(region, logger)
		resp, err := client.ListObjectVersions(context.TODO(), params)
		if err != nil {
			logger.Fatal("S3Bucket.Delete: ", err.Error())
		}
		// now delete each version
		for _, version := range resp.Versions {
			deleteVersion(region, bucketName, *version.Key, *version.VersionId, false, logger)
		}
		// delete each delete marker
		for _, deleteMarker := range resp.DeleteMarkers {
			deleteVersion(region, bucketName, *deleteMarker.Key, *deleteMarker.VersionId, false, logger)
		}
		keyMarker = *resp.KeyMarker
		if keyMarker == "" {
			break
		}
	}
}

// returns false if bucket doesn't exist or don't have permissions
func doesExist(region, bucketName string, logger *Logger) bool {
	client := getClient(region, logger)
	// create the corresponding s3 bucket
	params := &s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	}
	// Head Bucket will return no nil if either doesn't exist or
	_, err := client.HeadBucket(context.TODO(), params)
	if err != nil {
		return false
	}
	return true
}

func getClient(region string, logger *Logger) *s3.Client {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		logger.Fatal("unable to create s3 session")
	}
	return s3.NewFromConfig(cfg, func(options *s3.Options) {
		options.Region = region
	})
}
func deleteVersion(region, bucketName, objectName, versionID string, sleep bool, logger *Logger) {
	logger.Event("Deleting version ", versionID, " of object ", objectName, " from bucket ", bucketName)
	client := getClient(region, logger)
	params := &s3.DeleteObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectName),
		VersionId: aws.String(versionID),
	}
	_, err := client.DeleteObject(context.TODO(), params)

	if err != nil {
		logger.Fatal("DeleteVersion: ", err.Error())
	}
	// need to sleep so that delete marker is created before next version
	if sleep {
		time.Sleep(1 * time.Second)
	}
}
