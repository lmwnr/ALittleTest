package agent

import (
	"bytes"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
)

var (
	s3c *s3.S3
)

func init() {
	//httpc := *http.DefaultClient
	//httpc.Timeout = time.Second * 2
	//sess, err := session.NewSession(
	//	&aws.Config{
	//		Region:      aws.String("ap-south-1"),
	//		Credentials: credentials.NewStaticCredentials(conf.D.AWSKeyId, conf.D.AWSSecretKey, ""),
	//		HTTPClient:  &httpc,
	//	},
	//)
	//if err != nil {
	//	panic(err)
	//}
	//
	//// s3 init
	//s3c = s3.New(sess)
}

func AwsS3GetFileList(bkt, prefix string) ([]string, error) {
	res := make([]string, 0)

	err := s3c.ListObjectsV2Pages(&s3.ListObjectsV2Input{
		Bucket: aws.String(bkt),
		Prefix: aws.String(prefix),
	}, func(lovo *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, item := range lovo.Contents {
			res = append(res, *item.Key)
		}
		return !lastPage
	})

	return res, err
}

func AwsS3GetObjReader(bkt, key string) (io.ReadCloser, error) {
	out, err := s3c.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bkt),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}

	return out.Body, err
}

// s3://mx-multi/anticheat/mxapilog/20211216.txt
func AwsS3UploadFile(bkt string, key string, data []byte) error {
	_, err := s3c.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bkt),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})

	return err
}
