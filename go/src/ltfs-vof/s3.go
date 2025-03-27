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

func Put(bucket, key, region string, prefix, block string) {

	client := getClient(region)
	// open the block file
	f, err := os.Open(prefix + "/" + bucket + "/" + block)
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
		Bucket: aws.String(bucket+"-test"),
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

	client := getClient(region)
	params := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket+"-test"),
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

	client := getClient(region)
	params := &s3.DeleteObjectInput{
		Bucket:    aws.String(bucket+"-test"),
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
func PutMultipart(bucket, key, region string, prefix string, blocks []string) {
	client := getClient(region)

	// input for starting a multipart upload
	input := s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket+"-test"),
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
		f, err := os.Open(prefix + "/" + bucket + "/" + block)
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
			Bucket:     aws.String(bucket+"-test"),
			Key:        aws.String(key),
			PartNumber: aws.Int32(int32(partNum+1)),
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
			PartNumber: aws.Int32(int32(partNum+1)),
		}
		partsInfo = append(partsInfo, partInfo)
	}

	// compelete the upload
	mpu := types.CompletedMultipartUpload{
		Parts: partsInfo,
	}

	complete := s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(bucket+"-test"),
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
