package snapshot

import (
	"bytes"
	"context"
	"fmt"
	"go.uber.org/zap"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/criticalstack/e2d/pkg/log"
	e2daws "github.com/criticalstack/e2d/pkg/provider/aws"
	"github.com/pkg/errors"
)

func newAWSConfig(name string) (*aws.Config, error) {
	if name != "" {
		return e2daws.NewConfigWithRoleSession(name)
	}
	return e2daws.NewConfig()
}

type AmazonConfig struct {
	RoleSessionName string
	Bucket          string
	Key             string
	RetentionDays   int64
}

type AmazonSnapshotter struct {
	*s3.S3
	*s3manager.Downloader
	*s3manager.Uploader

	bucket, key string
}

func NewAmazonSnapshotter(cfg *AmazonConfig) (*AmazonSnapshotter, error) {
	awsCfg, err := newAWSConfig(cfg.RoleSessionName)
	if err != nil {
		return nil, err
	}
	return newAmazonSnapshotter(awsCfg, cfg.Bucket, cfg.Key, cfg.RetentionDays)
}

func newAmazonSnapshotter(cfg *aws.Config, bucket, key string, retentionDays int64) (*AmazonSnapshotter, error) {
	sess, err := session.NewSession(cfg)
	if err != nil {
		return nil, err
	}
	s3conn := s3.New(sess)
	s := &AmazonSnapshotter{
		S3:         s3conn,
		Downloader: s3manager.NewDownloader(sess),
		Uploader:   s3manager.NewUploader(sess),
		bucket:     bucket,
		key:        key,
	}

	// Ensure that the bucket exists
	req, _ := s.HeadBucketRequest(&s3.HeadBucketInput{
		Bucket: aws.String(s.bucket),
	})
	err = req.Send()
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

	// optionally setup retention
	if retentionDays > 0 {
		// TODO: figure out how to prevent deleting snapshots from s3 if etcd hasn't written a snapshot in a while
		input := &s3.PutBucketLifecycleConfigurationInput{
			Bucket: aws.String(bucket),
			LifecycleConfiguration: &s3.BucketLifecycleConfiguration{
				Rules: []*s3.LifecycleRule{
					{
						Expiration: &s3.LifecycleExpiration{
							Days: aws.Int64(retentionDays),
						},
						Filter: &s3.LifecycleRuleFilter{
							Prefix: aws.String(key),
						},
						ID:     aws.String(fmt.Sprintf("E2DLifecycle-%s", key)),
						Status: aws.String("Enabled"),
					},
				},
			},
		}

		_, err := s3conn.PutBucketLifecycleConfiguration(input)
		if err != nil {
			return nil, errors.Wrap(err, "unable to put bucket lifecycle policy")
		}
	}

	return s, nil
}

func (s *AmazonSnapshotter) Load() (io.ReadCloser, error) {
	tmpFile, err := ioutil.TempFile("", "snapshot.download")
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	// generate the filename to the snapshot pointer file
	latestPath := s.key + fmt.Sprintf("%s.%s", snapshotFilename, latestSuffix)

	// download the latest snapshot pointer file
	var latestFilePath string
	buf := aws.NewWriteAtBuffer([]byte{})
	if _, err = s.DownloadWithContext(ctx, buf, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(latestPath),
	}); err == nil {
		l := &LatestFile{}
		err := l.read(buf.Bytes())
		if err != nil {
			return nil, errors.Wrap(err, "unable to unmarshal latest backup pointer file")
		}
		log.Debug("Received latestFile", zap.String("path", l.Path), zap.String("timestamp", l.Timestamp))
		latestFilePath = l.Path
	} else {
		return nil, errors.Wrap(err, "unable to retrieve latest backup pointer file")
	}

	// download the latest snapshot
	if _, err = s.DownloadWithContext(ctx, tmpFile, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(latestFilePath),
	}); err != nil {
		tmpFile.Close()
		return nil, errors.Wrapf(err, "cannot download file: %v", s.key)
	}
	if _, err := tmpFile.Seek(0, 0); err != nil {
		return nil, err
	}
	return tmpFile, nil
}

func (s *AmazonSnapshotter) Save(r io.ReadCloser) error {
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	// generate the filenames
	backupTimestamp := time.Now().UTC()
	snapshotPath := s.key + fmt.Sprintf("%s.%d", snapshotFilename, backupTimestamp.Unix())
	latestPath := s.key + fmt.Sprintf("%s.%s", snapshotFilename, latestSuffix)

	// upload the snapshot itself
	_, err := s.UploadWithContext(ctx, &s3manager.UploadInput{
		Body:   r,
		Bucket: aws.String(s.bucket),
		Key:    aws.String(snapshotPath),
	})
	if err != nil {
		return err
	}

	// upload the latest snapshot pointer file
	latestFile := &LatestFile{
		Path: snapshotPath,
		Timestamp: backupTimestamp.Format("2006-01-02T15:04:05-0700"),
	}
	latestContent, err := latestFile.generate()
	if err != nil {
		return err
	}
	_, err = s.UploadWithContext(ctx, &s3manager.UploadInput{
		Body:	bytes.NewReader(latestContent),
		Bucket: aws.String(s.bucket),
		Key:	aws.String(latestPath),
	})
	return err
}
