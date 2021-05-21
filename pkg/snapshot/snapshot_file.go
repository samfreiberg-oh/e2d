package snapshot

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/criticalstack/e2d/pkg/log"
	"github.com/pkg/errors"
)

type FileSnapshotter struct {
	path string
	retentionTime time.Duration
}

func NewFileSnapshotter(path string, retentionTime time.Duration) (*FileSnapshotter, error) {
	// TODO: check if path is a directory

	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil && !os.IsExist(err) {
		return nil, errors.Wrapf(err, "cannot create snapshot directory: %#v", filepath.Dir(path))
	}
	return &FileSnapshotter{path: path, retentionTime: retentionTime}, nil
}

func (fs *FileSnapshotter) Load() (io.ReadCloser, error) {
	// read the latest symlink
	latestSymlink := filepath.Join(fs.path, fmt.Sprintf("%s.%s", snapshotFilename, latestSuffix))
	return os.Open(latestSymlink)
}

func (fs *FileSnapshotter) Save(r io.ReadCloser) error {
	defer r.Close()

	// generate the filenames
	backupTimestamp := time.Now().UTC()
	snapshotFile := filepath.Join(fs.path, fmt.Sprintf("%s.%s", snapshotFilename, strconv.FormatInt(backupTimestamp.Unix(), 10)))
	latestSymlink := filepath.Join(fs.path, fmt.Sprintf("%s.%s", snapshotFilename, latestSuffix))

	// make the snapshot
	f, err := os.OpenFile(snapshotFile, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	// update the symlink to point to the latest snapshot
	if _, err := os.Lstat(latestSymlink); err == nil {
		err = os.Remove(latestSymlink)
		if err != nil {
			return errors.Wrap(err, "can't remove latest symlink")
		}
	}
	if err = os.Symlink(snapshotFile, latestSymlink); err != nil {
		return errors.Wrap(err, "can't create latest symlink")
	}

	_, err = io.Copy(f, r)

	// purge old snapshots
	if fs.retentionTime > 0 {
		files, err := ioutil.ReadDir(fs.path)
		if err != nil {
			return errors.Wrap(err, "unable to list snapshot directory during pruning")
		}
		for _, f := range files {
			if (f.Mode()&os.ModeSymlink != os.ModeSymlink) && strings.HasPrefix(f.Name(), snapshotFilename) && time.Now().Sub(f.ModTime()) > fs.retentionTime {
				// prune the file
				log.Warnf("Would have deleted %s", f.Name())
				//_ = os.Remove(f.Name())
			}
		}
	}

	return err
}
