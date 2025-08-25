package store

import (
	"bytes"
	"os"
	"path"
	"testing"
)

// =============== // MAIN SETUP // ===============

// Useful to have when you want to do specific setup and breakdown of the test
func TestMain(m *testing.M) {
	// Run tests
	code := m.Run()

	// Cleanup if needed
	os.Exit(code)
}

// =============== // OTHER HELPERS // ===============

func TestFormatBytes(t *testing.T) {
	// Double-checked here:
	// https://www.flexinput.com/tools/converters/size-in-bytes-as-human-readable-text/
	tests := []struct {
		bytes    int64
		expected string
	}{
		{0, "0 B"},
		{1, "1 B"},
		{512, "512 B"},
		{1023, "1023 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{2048, "2.0 KB"},
		{1048576, "1.0 MB"},
		{1572864, "1.5 MB"},
		{1073741824, "1.0 GB"},
		{1099511627776, "1.0 TB"},
		{1125899906842624, "1.0 PB"},
		{5368709120, "5.0 GB"},
		{123456789, "117.7 MB"},
	}

	for _, test := range tests {
		result := FormatBytes(test.bytes)
		if result != test.expected {
			t.Errorf("FormatBytes(%d): expected %q, got %q", test.bytes, test.expected, result)
		}
	}
}

func TestCRUD(t *testing.T) {
	h := NewTestHelper(t)
	s := NewStore(h.Client, h.BucketName, h.TestPrefix)

	const (
		fileName      = "testUpload.txt"
		fileName2     = "file2.txt"
		renamedFile2  = "file2-renamed.txt"
		dirName       = "testDir"
		fileContents  = "this is a test upload check"
		file2Contents = "second test file"
	)

	// =============== // CREATE // ===============
	t.Run("Upload File", func(t *testing.T) {
		// We mock a file upload
		// A handler will get the file from something like:
		// file, header, err := r.FormFile("uploadfile")
		// Where file is a io.Reader
		uploadedFileBytes := []byte(fileContents)
		written, err := s.UploadFile(h.Context, bytes.NewReader(uploadedFileBytes), "", fileName)

		// Make sure we get no error
		if err != nil {
			t.Fatalf("Failed to upload file: %v", err)
		}
		t.Logf("Uploaded %s", FormatBytes(written))

		// Verify file is there
		if !h.VerifyFile(path.Join(h.TestPrefix, fileName)) {
			t.Fatalf("Uploaded file not found")
		}

		// Very file contents
		if !h.VerifyFileContents(path.Join(h.TestPrefix, fileName), fileContents) {
			t.Fatalf("Uploaded file contents do not match")
		}
	})

	t.Run("Create Directory", func(t *testing.T) {
		err := s.CreateDirectory(h.Context, "", dirName)

		if err != nil {
			t.Fatalf("Failed to create directory: %v", err)
		}

		if !h.VerifyDirectory(path.Join(h.TestPrefix, dirName)) {
			t.Fatalf("Created directory not found")
		}
	})

	if t.Failed() {
		t.Fatal("Skipping remaining tests since we could not upload a file")
	}

	// =============== // READ // ===============

	t.Run("Lits objects and files", func(t *testing.T) {
		objects, _, _, err := s.ListPaginatedObjects(h.Context, "", "", 10)
		if err != nil {
			t.Fatalf("Failed to list objects: %v", err)
		}

		if len(objects) != 2 {
			t.Fatalf("Expected to find two objects, got %d", len(objects))
		}

		var foundFile, foundDir bool
		for _, obj := range objects {
			t.Logf("- %s (isDir: %v, size: %s, created: %v, updated: %v)", obj.Name, obj.IsDir, obj.HumanReadableSize, obj.Created, obj.Updated)

			if obj.Name == fileName && !obj.IsDir {
				foundFile = true
			}
			if obj.Name == dirName && obj.IsDir {
				foundDir = true
			}
		}

		if !foundFile {
			t.Errorf("Expected to find file %q in objects list", fileName)
		}
		if !foundDir {
			t.Errorf("Expected to find directory %q in objects list", dirName)
		}
	})

	t.Run("List objects with pagination requiring two calls", func(t *testing.T) {
		// Create an additional file to ensure we have enough objects for pagination
		_, err := s.UploadFile(h.Context, bytes.NewReader([]byte(file2Contents)), "", fileName2)
		if err != nil {
			t.Fatalf("Failed to upload second file: %v", err)
		}

		// First call with limit 1 to force pagination
		objects1, lastObjectName, hasMore, err := s.ListPaginatedObjects(h.Context, "", "", 1)
		if err != nil {
			t.Fatalf("Failed to list objects (first page): %v", err)
		}

		if len(objects1) != 1 {
			t.Fatalf("Expected to find one object on first page, got %d", len(objects1))
		}

		t.Logf("First page object: %s (isDir: %v, size: %s)", objects1[0].Name, objects1[0].IsDir, objects1[0].HumanReadableSize)

		if !hasMore {
			t.Fatalf("Expected hasMore to be true after first page")
		}

		if lastObjectName == "" {
			t.Fatalf("Expected lastObjectName to be set after first page")
		}

		// Second call to get remaining objects
		objects2, _, hasMore2, err := s.ListPaginatedObjects(h.Context, "", lastObjectName, 10)
		if err != nil {
			t.Fatalf("Failed to list remaining objects: %v", err)
		}

		t.Logf("Second call found %d remaining objects", len(objects2))
		for i, obj := range objects2 {
			t.Logf("Remaining object %d: %s (isDir: %v)", i, obj.Name, obj.IsDir)
		}

		if hasMore2 {
			t.Errorf("Expected no more objects after getting all remaining objects")
		}

		// Verify we got all objects across both calls
		allObjects := append(objects1, objects2...)
		totalFound := len(allObjects)

		t.Logf("Total objects found across two paginated calls: %d", totalFound)

		if totalFound < 3 { // At least original file + dir + new file
			t.Errorf("Expected at least 3 objects total, got %d", totalFound)
		}
	})

	// =============== // UPDATE // ===============

	t.Run("Rename File", func(t *testing.T) {
		// Rename the original file
		err := s.RenameObject(h.Context, "", fileName2, "", renamedFile2)
		if err != nil {
			t.Fatalf("Failed to rename file: %v", err)
		}

		// Verify the original file no longer exists
		if h.VerifyFile(path.Join(h.TestPrefix, fileName2)) {
			t.Errorf("Original file %q should not exist after rename", fileName2)
		}

		// Verify the renamed file exists
		if !h.VerifyFile(path.Join(h.TestPrefix, renamedFile2)) {
			t.Fatalf("Renamed file %q not found", renamedFile2)
		}

		// Verify file contents are preserved
		if !h.VerifyFileContents(path.Join(h.TestPrefix, renamedFile2), file2Contents) {
			t.Fatalf("Renamed file contents do not match original")
		}
	})

	// =============== // DELETE (VERSION CONTROL) // ===============
}
