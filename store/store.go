package store

import (
	"context"
	"fmt"
	"io"
	"path"

	"cloud.google.com/go/storage"
)

// The Store Type
// It's a wrapper around any filestore like GCS or S3
// The client must not be created per request due to the overhead of:
// 1) Authentication setup - OAuth token exchange/validation
// 2) Connection establishment - Network handshake with Google's APIs
// 3)HTTP client pool initialization - Setting up connection pools
// Instead, create the client with the context of the application
// All other operations will use r.Context for the context
// The documentation mentions "connection pooling" as a feature handled
// by the client Google CloudGo Packages - this means the client maintains
// persistent connections that are reused.
type Store struct {
	Client     *storage.Client
	BucketName string
	BasePrefix string
}

// A function to pretty print bytes
func FormatBytes(bytes int64) string {
	if bytes < 1024 {
		return fmt.Sprintf("%d B", bytes)
	}

	units := []string{"KB", "MB", "GB", "TB", "PB"}
	size := float64(bytes)

	for _, unit := range units {
		size /= 1024
		if size < 1024 {
			return fmt.Sprintf("%.1f %s", size, unit)
		}
	}

	return fmt.Sprintf("%.1f %s", size, units[len(units)-1])
}

// Creates a new Store Instance
// The bucketName is where the files will be stored
// It's optional to add a basPrefix. It's useful for tests
func NewStore(client *storage.Client, bucketName, basePrefix string) *Store {
	return &Store{
		Client:     client,
		BucketName: bucketName,
		BasePrefix: basePrefix,
	}
}

func (s *Store) UploadFile(ctx context.Context, reader io.Reader, prefix, filename string) (written int64, err error) {
	obj := s.GetObject(s.BasePrefix, prefix, filename)

	writer := obj.NewWriter(ctx)
	defer writer.Close()
	if written, err := io.Copy(writer, reader); err != nil {
		return 0, err
	} else {
		return written, nil
	}
}

func (s *Store) getBucket() *storage.BucketHandle {
	return s.Client.Bucket(s.BucketName)
}

func (s *Store) getObject(objectPath string) *storage.ObjectHandle {
	bkt := s.getBucket()
	return bkt.Object(objectPath)
}

func (s *Store) GetObject(parts ...string) *storage.ObjectHandle {
	fullPath := path.Join(parts...)
	return s.getObject(fullPath)
}
