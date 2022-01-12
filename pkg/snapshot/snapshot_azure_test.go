package snapshot

import (
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"
)

func TestParseAzureURL(t *testing.T) {
	tests := []struct {
		have string
		san  string // storage account name
		cn   string // container name
		err  error
	}{
		{"azure://mystorageaccount.blob.core.windows.net/my-container", "mystorageaccount.blob.core.windows.net", "my-container", nil},
		{"azure:///my-container", "", "", AzureHostEmptyError},
		{"http://example.com/my-container", "", "", AzureUnsupportedSchemeError},
		{"azure://mystorageaccount.blob.core.windows.net/", "", "", AzurePathEmptyError},
	}

	for _, test := range tests {
		san, cn, err := ParseAzureURL(test.have)
		if san != test.san || cn != test.cn || err != test.err {
			t.Fatalf(
				"ParseAzureURL(%q) = %s, %s, %v; expected %s, %s, %v\n",
				test.have,
				san,
				cn,
				err,
				test.san,
				test.cn,
				test.err,
			)
		}
	}
}

// TestSnapshot is an end to end test that does the following:
//   1. Uploads a "backup" including the pointer file that points to this as the latest.
//   2. Download the "backup" and compare it to what was written. This also reads the latest file to get the latest file.
func TestSnapshot(t *testing.T) {
	have := time.Now().Format(time.RFC3339Nano)
	want := have

	getOrFail := func(t *testing.T, name string) string {
		val := os.Getenv(name)
		if val == "" {
			t.Fatalf("%s is unset. Please set the env variable appropriately", name)
		}
		return val
	}

	rawURL := getOrFail(t, "E2D_SNAPSHOT_BACKUP_URL")
	san, cn, err := ParseAzureURL(rawURL)
	if err != nil {
		t.Fatalf("unable to parse Azure URL %q: %s\n", rawURL, err)
	}

	c := &AzureConfig{
		StorageAccount: san,
		ContainerName:  cn,
		AccountName:    getOrFail(t, "E2D_AZURE_ACCOUNT_NAME"),
		AccountKey:     getOrFail(t, "E2D_AZURE_ACCOUNT_KEY"),
	}

	snapshotter, err := NewAzureSnapshotter(c)
	if err != nil {
		t.Fatalf("Error getting Azure snapshotter: %s\n", err)
	}

	rc := io.NopCloser(strings.NewReader(have))
	if err := snapshotter.Save(rc); err != nil {
		t.Fatalf("Error saving snapshot in Azure: %s\n", err)
	}

	reader, err := snapshotter.Load()
	if err != nil {
		t.Fatalf("Error loading data: %s\n", err)
	}

	got, err := ioutil.ReadAll(reader)
	if err != nil || want != string(got) {
		t.Fatalf("Snapshotter.Load() = %v, %v; wanted %v, <nil>\n", string(got), err, want)
	}
}
