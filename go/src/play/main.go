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
	"math"
	"os"
)

func main() {
	region := "us-west-1"           // region
	bucket := "geyser-vailbucket-1" // bucket
	fname := "./play"
	maxPartSize := 5 * 1024 * 1024 // 5 MB
	maxThreads := 1

	customMultipartUpload(region, fname, bucket, maxPartSize, maxThreads)

}
func Put(bucket, key, region string, block string) {

	bucket = bucket

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
	fmt.Println("data size: ", len(data))
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

func getClient(region string) *s3.Client {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal("unable to create s3 session")
	}
	return s3.NewFromConfig(cfg, func(options *s3.Options) {
		options.Region = region
	})
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func customMultipartUpload(region, path, bucket string, maxPartSize, maxThreads int) {

	// open the block file
	f, err := os.Open(path)
	if err != nil {
		log.Fatal("Unable to open block for single block upload: ", err)
	}
	defer f.Close()

	// create a slice of the size of the parts
	fileInfo, err := os.Stat(path)
	check(err)
	var partSizes []int
	for i := 0; i < fileInfo.Size(); i += maxPartSize { 
		if (i + maxPartSize) > fileSize {
			partSizes = append(partSizes, fileSize-i)
		} else {	
			partSizes = append(partSizes, maxPartSize)
		}
	}
	// 

	// Create client and start multipart upload
	client := getClient(region)
	multipartInput := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(path),
	}
	uploaderInfo, err := client.CreateMultipartUpload(context.TODO(), multipartInput)
	check(err)

	// reserve the number of goroutines that can run concurrently
	activeGoRoutines := NewResource(maxThreads)
	// go through the parts and reserve a goroutine for each part up to the number of threads
	start := 0
	for partNum, size := range partSizes {
		// read the part from the file
		start += size
		data := make([]byte, size)
		_, err := f.ReadAt(data, int64(start))
		check(err)

		// reserve a goroutine
		activeRoutines.Reserve()
		go func(partNum int, data []byte, bucket string, resource *Resource) {
			partInput := &s3.UploadPartInput{
				Body:       bytes.NewReader(data),
				Bucket:     aws.String(bucket),
				Key:        aws.String(path),
				PartNumber: &partNumber,
				UploadId:   uploaderInfo.UploadId,
			}

			partUploadResult, err := client.UploadPart(context.TODO(), partInput)
			if err != nil {
				println("Unable to upload part: ", err)
					abortInput := &s3.AbortMultipartUploadInput{
						Bucket:   aws.String(bucket),
						Key:      aws.String(path),
						UploadId: uploaderInfo.UploadId,
					}
					_, abortErr := client.AbortMultipartUpload(context.TODO(), abortInput)
					if abortErr != nil {
						log.Fatal(abortErr)
					}
					log.Fatal(err)
				}

				partOutput := types.CompletedPart{
					ETag:       partUploadResult.ETag,
					PartNumber: &currPartNumber,
				}
				// release the reservation on this go routine
				activeRoutines.Release()
			}(partNum, data, bucket, activeRoutines)

		}

		for range numPartsOut {
			currOutput := <-chans
			println("completed part:", *currOutput.PartNumber, "/", estimatedPartCount)
			completedParts[*currOutput.PartNumber-1] = currOutput
		}
	}

	//test the completed parts are in order (they should be).
	//println("checking order of completed parts")
	//for _, part := range completedParts {
	//            println(*part.PartNumber)
	//}

	// Wrap it all up
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(path),
		UploadId: uploaderInfo.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}
	_, completeError := client.CompleteMultipartUpload(context.TODO(), completeInput)
	check(completeError)
	println("completed uploading all parts")

}
