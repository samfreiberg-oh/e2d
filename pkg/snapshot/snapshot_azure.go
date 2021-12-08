// TODO: Check if container exists and if not try to create it
package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

type azureSnapshotter struct {
	config    *AzureConfig
	container azblob.ContainerClient
}

// AzureConfig contains the configuration options for storing database
// snapshots in Azure Storage accounts.
type AzureConfig struct {
	// AccountName is the Azure account name.
	AccountName string

	// AccountKey is the secret to access the storage account.
	AccountKey string

	// StorageAccount is the storage account name.
	StorageAccount string

	// ContainerName is the top level namespace where we'll keep snapshots
	ContainerName string

	// Timeout controls how long we wait for Snapshotter.Save or Snapshotter.Load
	// to finish.
	Timeout time.Duration

	// Maximum number of times to retry an upload or download.
	Retries int
}

// NewAzureSnapshotter takes a pointer to AzureConfig and returns a type that
// satifies the Snapshotter interface.
func NewAzureSnapshotter(config *AzureConfig) (Snapshotter, error) {
	cred, err := azblob.NewSharedKeyCredential(config.AccountName, config.AccountKey)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("https://%s.blob.core.windows.net/", config.StorageAccount)

	client, err := azblob.NewServiceClientWithSharedKey(url, cred, nil)
	if err != nil {
		return nil, err
	}

	snapshotter := &azureSnapshotter{
		config:    config,
		container: client.NewContainerClient(config.ContainerName),
	}
	return snapshotter, nil
}

func (s *azureSnapshotter) Load() (io.ReadCloser, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.getTimeout())
	defer cancel()

	// get latest
	latest, err := s.getLatestSnapshotInfo(ctx)
	if err != nil {
		return nil, err
	}

	client := s.container.NewBlobClient(latest.Path)
	resp, err := client.Download(ctx, nil)
	if err != nil {
		return nil, err
	}

	body := resp.Body(s.azReaderOptions())
	return body, nil
}

func (s *azureSnapshotter) azReaderOptions() azblob.RetryReaderOptions {
	return azblob.RetryReaderOptions{MaxRetryRequests: s.config.Retries}
}

func (s *azureSnapshotter) getLatestSnapshotInfo(ctx context.Context) (*LatestFile, error) {
	client := s.container.NewBlobClient(s.latestPath())
	resp, err := client.Download(ctx, nil)
	if err != nil {
		return nil, err
	}

	latest := &LatestFile{}
	body := resp.Body(s.azReaderOptions())
	defer body.Close()

	if err := json.NewDecoder(body).Decode(latest); err != nil {
		return nil, err
	}
	return latest, nil
}

func (s *azureSnapshotter) Save(r io.ReadCloser) error {
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), s.getTimeout())
	defer cancel()

	// generate the filenames
	backedupAt := time.Now()
	snapshotPath := s.snapshotPath(backedupAt)

	// Write snapshot
	tmp, err := os.CreateTemp("", snapshotPath)
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())

	if _, err := io.Copy(tmp, r); err != nil {
		return err
	}

	if _, err := s.uploadFile(ctx, snapshotPath, tmp); err != nil {
		return err
	}

	_, err = s.updateLatest(ctx, snapshotPath, backedupAt)
	return err
}

func (s *azureSnapshotter) uploadFile(ctx context.Context, path string, file *os.File) (*http.Response, error) {
	opts := azblob.HighLevelUploadToBlockBlobOption{}
	client := s.container.NewBlockBlobClient(path)
	return client.UploadFileToBlockBlob(ctx, file, opts)
}

func (s *azureSnapshotter) uploadBytes(ctx context.Context, path string, p []byte) (*http.Response, error) {
	opts := azblob.HighLevelUploadToBlockBlobOption{}
	client := s.container.NewBlockBlobClient(path)
	return client.UploadBufferToBlockBlob(ctx, p, opts)
}

func (s *azureSnapshotter) updateLatest(ctx context.Context, path string, backedupAt time.Time) (*http.Response, error) {
	latest := &LatestFile{
		Path:      path,
		Timestamp: backedupAt.Format("2006-01-02T15:04:05-0700"),
	}
	out, err := latest.generate()
	if err != nil {
		return nil, err
	}

	return s.uploadBytes(ctx, s.latestPath(), out)
}

func (s *azureSnapshotter) latestPath() string {
	return fmt.Sprintf("%s.%s", snapshotFilename, latestSuffix)
}

func (s *azureSnapshotter) snapshotPath(backedupAt time.Time) string {
	return fmt.Sprintf("%s.%d", snapshotFilename, backedupAt.Unix())
}

func (s *azureSnapshotter) getTimeout() time.Duration {
	if s.config.Timeout != 0 {
		return s.config.Timeout
	}
	return time.Duration(time.Minute)
}
