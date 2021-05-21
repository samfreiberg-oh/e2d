package snapshot

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
)

func TestParseSnapshotBackupURL(t *testing.T) {
	tests := []struct {
		name        string
		url         string
		expected    *URL
		expectedErr error
	}{
		{
			name:        "empty",
			url:         "",
			expected:    nil,
			expectedErr: ErrInvalidScheme,
		},
		{
			name:     "local directory at root",
			url:      "file:///",
			expected: &URL{Type: FileType, Path: "/"},
		},
		{
			name:     "local file path (should fail)",
			url:      "file://abc",
			//expected: &URL{Type: FileType, Path: "abc"},
			expectedErr: ErrInvalidDirectoryPath,
		},
		{
			name:     "local directory",
			url:      "file://abc/",
			expected: &URL{Type: FileType, Path: "abc"},
		},
		{
			name:     "local directory path with three slashes",
			url:      "file:///abc/",
			expected: &URL{Type: FileType, Path: "/abc"},
		},
		{
			name:     "s3 bucket with default name",
			url:      "s3://abc/",
			expected: &URL{Type: S3Type, Bucket: "abc", Path: ""},
		},
		{
			name:     "s3 bucket with prefix",
			url:      "s3://abc/backupdir/",
			expected: &URL{Type: S3Type, Bucket: "abc", Path: "backupdir/"},
		},
		{
			name:     "s3 with no directory (should fail)",
			url:      "s3://abc/backupdir",
			//expected: &URL{Type: S3Type, Bucket: "abc", Path: "backupdir"},
			expectedErr: ErrInvalidDirectoryPath,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := ParseSnapshotBackupURL(tt.url)
			if err != nil && errors.Cause(err) != tt.expectedErr {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.expected, u); diff != "" {
				t.Errorf("snapshot: after Parse differs: (-want +got)\n%s", diff)
			}
		})
	}
}
