package digitalocean

import (
	"bytes"
	"context"
	"net/http"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pkg/errors"
)

// ObjectStore implements the provider.ObjectStore interface for AWS S3.
type ObjectStore struct {
	*s3.S3
	*s3manager.Uploader
	*s3manager.Downloader

	bucket string
}

func NewObjectStore(cfg *Config, bucket string) (*ObjectStore, error) {
	s3Config := &aws.Config{
		Credentials: credentials.NewStaticCredentials(cfg.SpacesAccessKey, cfg.SpacesSecretKey, ""),
		Endpoint:    aws.String(cfg.SpacesURL),
		Region:      aws.String("us-east-1"), // This is counter intuitive, but it will fail with a non-AWS region name.
	}
	s := session.New(s3Config)
	objs := &ObjectStore{
		S3:         s3.New(s),
		Uploader:   s3manager.NewUploader(s),
		Downloader: s3manager.NewDownloader(s),
		bucket:     bucket,
	}

	// Ensure that the bucket exists
	req, _ := objs.HeadBucketRequest(&s3.HeadBucketInput{
		Bucket: aws.String(objs.bucket),
	})
	err := req.Send()
	if err != nil {
		if reqErr, ok := err.(awserr.RequestFailure); ok {
			switch reqErr.StatusCode() {
			case http.StatusNotFound:
				return nil, errors.Errorf("bucket %s does not exist", bucket)
			case http.StatusForbidden:
				return nil, errors.Errorf("access to bucket %s forbidden", bucket)
			default:
				return nil, errors.Errorf("bucket could not be accessed: %v", err)
			}
		}
	}
	return objs, nil
}

// Exists checks for existence of a particular key in the established object
// bucket.
func (s *ObjectStore) Exists(ctx context.Context, key string) (bool, error) {
	req, _ := s.HeadObjectRequest(&s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	err := req.Send()
	if err != nil {
		if reqErr, ok := err.(awserr.RequestFailure); ok {
			if reqErr.StatusCode() == http.StatusNotFound {
				return false, nil
			}
			return false, err
		}
		return false, err
	}
	return true, nil
}

// Download downloads an object given the provided key.
func (s *ObjectStore) Download(ctx context.Context, key string) ([]byte, error) {
	buf := aws.NewWriteAtBuffer([]byte{})
	_, err := s.Downloader.DownloadWithContext(ctx, buf, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, errors.Wrapf(err, "cannot download file: %v", key)
	}
	return buf.Bytes(), nil
}

// Upload uploads an object given the provided key and file content.
func (s *ObjectStore) Upload(ctx context.Context, key string, data []byte) error {
	_, err := s.Uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	return errors.Wrapf(err, "cannot upload file: %v", key)
}