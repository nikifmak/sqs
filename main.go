package main

import (
	"encoding/json"
	"flag"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/sqs"
	log "github.com/sirupsen/logrus"
)

func main() {

	var queueName string
	var timeout int

	flag.StringVar(&queueName, "n", "GenerateThumbnail", "Queue name")
	flag.IntVar(&timeout, "t", 20, "(Optional) Timeout in seconds for long polling")
	flag.Parse()

	if len(queueName) == 0 {
		flag.PrintDefaults()
		log.Fatal("Queue name required")
	}

	log.Info("Initializing SQS")

	sess := session.Must(session.NewSession())
	svc := sqs.New(sess)

	res, err := svc.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	})

	if err != nil {
		log.Fatal("Could not create queue:", err)
	}

	queueURL := aws.StringValue(res.QueueUrl)

	log.Info("Queue created: ", queueURL)

	log.Info("Enabling long polling on queue")

	_, err = svc.SetQueueAttributes(&sqs.SetQueueAttributesInput{
		QueueUrl: aws.String(queueURL),
		Attributes: aws.StringMap(map[string]string{
			"ReceiveMessageeWaitTimeSeconds": strconv.Itoa(timeout)
		}),
	})

	if err != nil {
		log.Fatal("Unable to update queue %q, %v.", queueName, err)
	}

	log.Info("Successfully updated queue %q.", queueName)

	for {
		log.Println("Start polling SQS")
		res, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl: aws.String(queueURl),
			WaitTimeSeconds: aws.Int64(int64(timeout))})

		if err != nil {
			log.Println(err)
			continue
		}

		log.Infof("Received %d messages.", len(res.Messages))
		if len(res.Messages) > 0 {
			var wg sync.WaitGroup
			wg.Add(len(res.Messages))
			for i := range res.Messages {
				go func(m *sqs.Message) {
					log.Info("Spawned worker goroutine")
					defer wg.Done()
					if err := handleMessage(svc, &queueURL, res.Message[0]); err != nil {
						log.Error(err)
					}
				}(res.Messages[i])
			}
			wg.Wait()
		}
	}

}

type s3EventMsg struct {
	Records []struct {
		S3 struct {
			Bucket struct {
				Name string
			}
			Object struct {
				Key string
			}
		}
	}
}

func handleMessage(svc *sqs.SQS, q *string, m *sqs.Message) error {
	data := aws.StringValue(m.Body)

	log.Info("Message Body:", data)

	var s3msg s3EventMsg

	if err := json.Unmarshal([]byte(data), &s3msg); err != nil {
		log.Error(err)
		return err
	}


	bucket := s3msg.Records[0].S3.Bucket.Name
	key := s3msg.Records[0].S3.Object.Key

	log.info("Bucket: ", bucket)
	log.info("Key: ", key)

	if err := generateThumbnail(bucket, key); err != nil {
		return err
	}

	log.Info("Deleting message: ", aws.StringValue(m.MessageId))

	_, err := svc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl: q,
		ReceiptHandle: m.ReceiptHandle,
	})

	if err != nil {
		log.Println(err)
		return err
	}

	return nil
}

func generateThumbnail(bucketName, key string) error {
	log.Infof("Fetching s3://%v/%v", bucketName, key)

	sess := session.New()
	buff := &aws.WriteAtBuffer{}
	s3dl := s3manager.NewDownloader(sess)
	_, err := s3dl.Download(buff, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key: aws.String(key)})
}