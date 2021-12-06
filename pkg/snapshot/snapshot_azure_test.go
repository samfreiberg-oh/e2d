package snapshot

import (
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"
)

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

	c := &AzureConfig{
		AccountName:    getOrFail(t, "E2D_AZURE_ACCOUNT_NAME"),
		AccountKey:     getOrFail(t, "E2D_AZURE_ACCOUNT_KEY"),
		StorageAccount: getOrFail(t, "E2D_AZURE_STORAGE_ACCOUNT"),
		ContainerName:  getOrFail(t, "E2D_AZURE_CONTAINER_NAME"),
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
	if err != nil {
		t.Fatalf("Error reading all of the data: %s\n", err)
	}

	if want != string(got) {
		t.Fatalf("Snapshotter.Load() = %v, %v; wanted %v, <nil>\n", string(got), err, want)
	}
}
