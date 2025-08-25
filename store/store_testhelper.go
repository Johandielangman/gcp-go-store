package store

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/oklog/ulid/v2"
)

type TestHelper struct {
	Client     *storage.Client
	BucketName string
	TestPrefix string
	Context    context.Context
	t          testing.TB
}

func newDefaultULID() string {
	source := rand.NewSource(time.Now().UnixNano())
	entropy := rand.New(source)

	ulid := ulid.MustNew(ulid.Timestamp(time.Now()), entropy)
	return strings.ToLower(ulid.String())
}

func NewTestHelper(t testing.TB) *TestHelper {
	ctx := context.Background()

	// ====> GET THE TEST BUCKET NAME
	// We explicitly use a different environment variable other than one
	// That would be used by production
	// you don't want to accidentally use the production one
	bucketName := os.Getenv("TEST_BUCKET_NAME")
	if bucketName == "" {
		t.Fatal("TEST_BUCKET_NAME environment variable must be set")
	}

	// Create a new client
	client, err := storage.NewClient(ctx)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Create a new prefix to use for our tests
	// https://www.usefulids.com/resources/generate-ulid-in-go
	// We use a ulid since it can be time-sorted
	// Really better than UUID in every way! https://ulidtool.net/
	testPrefix := fmt.Sprintf("test-%s", newDefaultULID())

	// Create the test directory
	bkt := client.Bucket(bucketName)
	obj := bkt.Object(testPrefix + "/")
	if err := obj.NewWriter(ctx).Close(); err != nil {
		t.Fatalf("Failed to create test prefix %q: %v", testPrefix, err)
	}

	helper := &TestHelper{
		Client:     client,
		BucketName: bucketName,
		TestPrefix: testPrefix,
		Context:    ctx,
		t:          t,
	}

	// With the t.Cleanup, and b.Cleanup methods, we get better control to
	// cleaning up after our tests. t.Cleanup registers a function to be called
	// when the test and all its subtests complete.
	// https://ieftimov.com/posts/testing-in-go-clean-tests-using-t-cleanup/
	t.Cleanup(func() {
		// First use the helper to clean up anything remaining
		helper.Cleanup()

		// Then finally close the client
		client.Close()
	})

	return helper
}

func (h *TestHelper) Cleanup() {
	// TODO - Add code that cleans the bucket by removing files
}

func (h *TestHelper) VerifyDirectory(objectName string) bool {
	if !strings.HasSuffix(objectName, "/") {
		objectName += "/"
	}
	return h.VerifyFile(objectName)
}

func (h *TestHelper) VerifyFile(objectName string) bool {
	obj := h.Client.Bucket(h.BucketName).Object(objectName)
	_, err := obj.Attrs(h.Context)
	if err != nil {
		// Check for object not exist errors more broadly
		if err == storage.ErrObjectNotExist || strings.Contains(err.Error(), "object doesn't exist") || strings.Contains(err.Error(), "notFound") {
			return false
		}
		h.t.Fatalf("Failed to get object attributes for %q: %v", objectName, err)
	}
	return true
}

func (h *TestHelper) VerifyFileContents(objectName string, expectedContents string) bool {
	// https://cloud.google.com/go/docs/reference/cloud.google.com/go/storage/latest#cloud_google_com_go_storage_ObjectHandle_NewReader

	rc, err := h.Client.Bucket(h.BucketName).Object(objectName).NewReader(h.Context)
	if err != nil {
		h.t.Fatalf("Failed to create reader for %q: %v", objectName, err)
		return false
	}

	slurp, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		h.t.Fatalf("Failed to read contents of %q: %v", objectName, err)
		return false
	}

	return string(slurp) == expectedContents
}
