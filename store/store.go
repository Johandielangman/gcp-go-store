package store

import (
	"context"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// ===================================
// THE STORE TYPE
// ===================================
//
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

// Google Cloud Storage also implements a retry
// Here: https://cloud.google.com/storage/docs/retry-strategy
// This is something that can be customized:
// https://cloud.google.com/go/docs/reference/cloud.google.com/go/storage/latest#hdr-Retrying_failed_requests
// Interesting enough, they don't configure a timeout.. it just always retries on the following codes:
// 408, 429, 500, 502, 503, 504 or connection errors sent by GCS
// These codes above are called non-idempotent retries!
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
func NewStore(
	client *storage.Client,
	bucketName, basePrefix string,
) *Store {
	return &Store{
		Client:     client,
		BucketName: bucketName,
		BasePrefix: basePrefix,
	}
}

// Since this struct HIGHLY depends on versioning being enabled, this function can
// Be called during app startup
// As per the docs, the below will "Enable versioning in the bucket, regardless of its previous value."
func EnableVersioning(
	ctx context.Context,
	client *storage.Client,
	bucketName string,
) error {
	// https://cloud.google.com/go/docs/reference/cloud.google.com/go/storage/latest#examples
	_, err := client.Bucket(bucketName).Update(ctx, storage.BucketAttrsToUpdate{VersioningEnabled: true})
	return err
}

// Create a custom struct used to list ObjectInformation
// GCP has an iterative approach to listing directories
// Which, in all honesty, is probably the best way to do it
// Why? Because what happens when you have a million files in a directory?
// To utilize this, I think we need to go for an approach like WhatsApp messages
// We show a certain limit... and at some point you can say "show more"
type ObjectInfo struct {
	Name              string    `json:"name"`
	IsDir             bool      `json:"is_dir"`
	Size              int64     `json:"size"`
	HumanReadableSize string    `json:"human_readable_size"`
	Created           time.Time `json:"created"`
	Updated           time.Time `json:"updated"`
}

// Uploads a file go GCS
// https://cloud.google.com/go/docs/reference/cloud.google.com/go/storage/latest#cloud_google_com_go_storage_ObjectHandle_NewWriter
// Why are we not specifying the content type?
// Attributes can be set on the object by modifying the returned Writer's ObjectAttrs
// field before the first call to Write. If no ContentType attribute is specified,
// the content type will be automatically sniffed using net/http.DetectContentType.

// Note that each Writer allocates an internal buffer of size Writer.ChunkSize
// ChunkSize controls the maximum number of bytes of the object that the
//
// Writer will attempt to send to the server in a single request. Objects
// smaller than the size will be sent in a single request, while larger
// objects will be split over multiple requests. The value will be rounded up
// to the nearest multiple of 256K. The default ChunkSize is 16MiB.
//
// Good reference to how the chunks and reties work:
// https://cloud.google.com/go/docs/reference/cloud.google.com/go/storage/latest#cloud_google_com_go_storage_Writer
func (s *Store) UploadFile(
	ctx context.Context,
	reader io.Reader,
	prefix, filename string,
) (
	written int64,
	err error,
) {
	obj := s.GetObject(s.BasePrefix, prefix, filename)

	writer := obj.NewWriter(ctx)
	defer writer.Close()

	// I set the size here in case we want to split it out
	writer.ChunkSize = 16 * 1024 * 1024

	if written, err := io.Copy(writer, reader); err != nil {
		return 0, err
	} else {
		return written, nil
	}
}

// There isn't actually such a thing as "creating a directory"
// Instead, you just create an empty object with a trailing slash
func (s *Store) CreateDirectory(
	ctx context.Context,
	prefix, dirName string,
) error {
	fullPath := path.Join(s.BasePrefix, prefix, dirName)
	if !strings.HasSuffix(fullPath, "/") {
		fullPath += "/"
	}
	obj := s.getObject(fullPath)

	writer := obj.NewWriter(ctx)
	return writer.Close()
}

func (s *Store) ListPaginatedObjects(
	ctx context.Context,
	prefix, startAfter string,
	limit int,
) (
	objects []ObjectInfo,
	lastObjectName string,
	hasMore bool,
	err error,
) {
	fullPrefix := path.Join(s.BasePrefix, prefix)

	// Add trailing slash to ensure we're listing within the directory
	if fullPrefix != "" && !strings.HasSuffix(fullPrefix, "/") {
		fullPrefix += "/"
	}

	// https://cloud.google.com/go/docs/reference/cloud.google.com/go/storage/latest#hdr-Listing_objects
	// https://cloud.google.com/go/docs/reference/cloud.google.com/go/storage/latest#cloud_google_com_go_storage_BucketHandle_Objects
	// https://cloud.google.com/storage/docs/samples/storage-list-files
	// Objects returns an iterator over the objects in the bucket that match the Query q.
	// If q is nil, no filtering is done. Objects will be iterated over lexicographically by name.
	// Note: The returned iterator is not safe for concurrent operations without explicit synchronization.
	it := s.getBucket().Objects(ctx, &storage.Query{
		Prefix:      fullPrefix,
		Delimiter:   "/", // NB: without this, we can't list "directories"
		StartOffset: startAfter,
	})

	count := 0

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, "", false, fmt.Errorf("error iterating objects: %v", err)
		}

		// Check if this is a directory prefix (returned by the delimiter)
		if attrs.Prefix != "" {
			// This is a directory
			name := attrs.Prefix
			if fullPrefix != "" && strings.HasPrefix(name, fullPrefix) {
				name = strings.TrimPrefix(name, fullPrefix)
			}
			name = strings.TrimSuffix(name, "/")

			if name != "" {
				objInfo := ObjectInfo{
					Name:              name,
					IsDir:             true,
					Size:              0,
					HumanReadableSize: "",
					Created:           time.Time{}, // Prefixes don't have timestamps
					Updated:           time.Time{},
				}
				objects = append(objects, objInfo)
				lastObjectName = attrs.Prefix
				count++

				// Check if we've reached the limit after processing
				if count >= limit {
					break
				}
			}
			continue
		}

		// This is a regular file
		// Remove the base prefix to get the relative name
		name := attrs.Name
		if fullPrefix != "" && strings.HasPrefix(name, fullPrefix) {
			name = strings.TrimPrefix(name, fullPrefix)
		}

		// Skip empty names (like the directory we're listing itself)
		if name == "" {
			continue
		}

		objInfo := ObjectInfo{
			Name:              name,
			IsDir:             false,
			Size:              attrs.Size,
			HumanReadableSize: FormatBytes(attrs.Size),
			Created:           attrs.Created,
			Updated:           attrs.Updated,
		}

		objects = append(objects, objInfo)
		lastObjectName = attrs.Name
		count++

		// Check if we've reached the limit after processing
		if count >= limit {
			break
		}
	}

	// Check if there are more objects by trying to get the next one
	hasMore = false
	if count >= limit {
		_, err := it.Next()
		if err != iterator.Done {
			hasMore = true
		}
	}

	return objects, lastObjectName, hasMore, nil
}

// RenameObject renames an object within the bucket by copying it to the new location
// and deleting the original. This follows GCS best practices since there is no native
// rename operation in Google Cloud Storage.
// No really:
// https://cloud.google.com/storage/docs/copying-renaming-moving-objects
// Both sourceObjectName and destinationObjectName should be relative to the basePrefix.
// VERY IMPORTANT!!! CAUTION!
// Caution: Because moving objects by copying them includes deleting the original objects
// from the source bucket, using this method to move objects whose storage class is Nearline storage,
// Coldline storage, or Archive storage can incur early deletion charges. If you move objects atomically,
// no early deletion charges are incurred, regardless of the storage class of the objects being moved.
// Since we don't use this, this shouldn't be a problem
func (s *Store) RenameObject(
	ctx context.Context,
	sourcePrefix, sourceObjectName string,
	destinationPrefix, destinationObjectName string,
) error {
	// Construct full paths
	sourcePath := path.Join(s.BasePrefix, sourcePrefix, sourceObjectName)
	destinationPath := path.Join(s.BasePrefix, destinationPrefix, destinationObjectName)

	// Get source and destination object handles
	srcObj := s.getObject(sourcePath)
	dstObj := s.getObject(destinationPath)

	// Set a generation-match precondition to avoid potential race conditions
	// and data corruptions. The request to copy is aborted if the object's
	// generation number does not match the precondition.
	// For a destination object that does not yet exist, set the DoesNotExist precondition.
	dstObj = dstObj.If(storage.Conditions{DoesNotExist: true})

	// Copy the object to the new location
	_, err := dstObj.CopierFrom(srcObj).Run(ctx)
	if err != nil {
		return fmt.Errorf("failed to copy object from %s to %s: %v", sourcePath, destinationPath, err)
	}

	// Delete the original object
	err = srcObj.Delete(ctx)
	if err != nil {
		// If deletion fails, we should try to clean up the copied object
		// to avoid leaving duplicate files
		if deleteErr := dstObj.Delete(ctx); deleteErr != nil {
			return fmt.Errorf("failed to delete source object %s and failed to cleanup destination object %s: original error: %v, cleanup error: %v", sourcePath, destinationPath, err, deleteErr)
		}
		return fmt.Errorf("failed to delete source object %s after copying: %v", sourcePath, err)
	}

	return nil
}

// Gets a bucket handle (private since it's intended to be a helper function)
func (s *Store) getBucket() *storage.BucketHandle {
	return s.Client.Bucket(s.BucketName)
}

// Using an objectPath, you can get an object
func (s *Store) getObject(objectPath string) *storage.ObjectHandle {
	bkt := s.getBucket()
	return bkt.Object(objectPath)
}

// The BEST way to handle paths
func (s *Store) GetObject(parts ...string) *storage.ObjectHandle {
	fullPath := path.Join(parts...)
	return s.getObject(fullPath)
}
