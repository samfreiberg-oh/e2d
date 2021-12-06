package snapshot

import (
	"encoding/json"
	"io"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
)

type Snapshotter interface {
	Load() (io.ReadCloser, error)
	Save(io.ReadCloser) error
}

var schemes = []string{
	"file://",
	"s3://",
	"http://",
	"https://",
}

func hasValidScheme(url string) bool {
	for _, s := range schemes {
		if strings.HasPrefix(url, s) {
			return true
		}
	}
	return false
}

type Type int

const (
	FileType Type = iota
	S3Type
	AzureType
)

const snapshotFilename = "etcd.snapshot"
const latestSuffix = "LATEST"

type URL struct {
	Type   Type
	Bucket string
	Path   string
}

var (
	ErrInvalidScheme        = errors.New("invalid scheme")
	ErrInvalidDirectoryPath = errors.New("path must be a directory")
	ErrCannotParseURL       = errors.New("cannot parse url")
)

type LatestFile struct {
	Path      string
	Timestamp string
}

func (l *LatestFile) generate() ([]byte, error) {
	content, err := json.Marshal(&l)
	return content, err
}

func (l *LatestFile) read(input []byte) error {
	return json.Unmarshal(input, l)
}

// ParseSnapshotBackupURL deconstructs a uri into a type prefix and a bucket
// example inputs and outputs:
//   file://file                                -> file://, file
//   s3://bucket                                -> s3://, bucket
//   azure://container							-> azure://, container_name
func ParseSnapshotBackupURL(s string) (*URL, error) {
	if !hasValidScheme(s) {
		return nil, errors.Wrapf(ErrInvalidScheme, "url does not specify valid scheme: %#v", s)
	}
	u, err := url.Parse(s)
	if err != nil {
		return nil, err
	}

	switch strings.ToLower(u.Scheme) {
	case "file":
		if !strings.HasSuffix(u.Path, string(filepath.Separator)) {
			return nil, ErrInvalidDirectoryPath
		}
		return &URL{
			Type: FileType,
			Path: filepath.Join(u.Host, u.Path),
		}, nil
	case "s3":
		path := strings.TrimPrefix(u.Path, "/")
		if !strings.HasSuffix(path, "/") && path != "" {
			return nil, ErrInvalidDirectoryPath
		}
		return &URL{
			Type:   S3Type,
			Bucket: u.Host,
			Path:   path,
		}, nil
	case "azure":
		return &URL{
			Type:   AzureType,
			Bucket: u.Host,
		}, nil
	}
	return nil, errors.Wrap(ErrCannotParseURL, s)
}
