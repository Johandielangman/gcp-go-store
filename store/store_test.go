package store

import (
	"bytes"
	"os"
	"path"
	"testing"
)

func TestMain(m *testing.M) {
	// Run tests
	code := m.Run()

	// Cleanup if needed
	os.Exit(code)
}

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

func TestCreate(t *testing.T) {
	h := NewTestHelper(t)
	s := NewStore(h.Client, h.BucketName, h.TestPrefix)

	// We mock a file upload
	// A handler will get the file from something like:
	// file, header, err := r.FormFile("uploadfile")
	// Where file is a io.Reader
	uploadedFileBytes := []byte("this is a very sensitive file")
	written, err := s.UploadFile(h.Context, bytes.NewReader(uploadedFileBytes), "", "secret.txt")

	// No error
	if err != nil {
		t.Fatalf("Failed to upload file: %v", err)
	}
	t.Logf("Uploaded %s", FormatBytes(written))

	// Verify file is there
	if !h.VerifyFile(path.Join(h.TestPrefix, "secret.txt")) {
		t.Fatalf("Uploaded file not found")
	}
}
