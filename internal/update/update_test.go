package update

import (
	"archive/tar"
	"compress/gzip"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestSanitizeTarPath(t *testing.T) {
	destDir := t.TempDir()

	tests := []struct {
		name      string
		path      string
		wantErr   bool
		skipOnWin bool
	}{
		{"normal file", "msgvault", false, false},
		{"nested file", "bin/msgvault", false, false},
		{"absolute path Unix", "/etc/passwd", true, false},
		{"path traversal with ..", "../../../etc/passwd", true, false},
		{"path traversal mid-path", "foo/../../../etc/passwd", true, false},
		{"hidden traversal", "foo/bar/../../..", true, false},
		{"dot only", ".", false, false},
		{"double dot only", "..", true, false},
		{"empty path", "", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skipOnWin && runtime.GOOS == "windows" {
				t.Skip("Unix-style absolute path not applicable on Windows")
			}
			_, err := sanitizeTarPath(destDir, tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("sanitizeTarPath(%q) error = %v, wantErr %v", tt.path, err, tt.wantErr)
			}
		})
	}
}

type archiveEntry struct {
	Name     string
	Content  string
	TypeFlag byte
	LinkName string
	Mode     int64
}

func createTestArchive(t *testing.T, path string, entries []archiveEntry) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	gzw := gzip.NewWriter(f)
	defer gzw.Close()
	tw := tar.NewWriter(gzw)
	defer tw.Close()

	for _, e := range entries {
		mode := e.Mode
		if mode == 0 {
			mode = 0644
		}
		h := &tar.Header{
			Name:     e.Name,
			Mode:     mode,
			Size:     int64(len(e.Content)),
			Typeflag: e.TypeFlag,
			Linkname: e.LinkName,
		}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if len(e.Content) > 0 {
			if _, err := tw.Write([]byte(e.Content)); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestExtractTarGzPathTraversal(t *testing.T) {
	tmpDir := t.TempDir()
	archivePath := filepath.Join(tmpDir, "malicious.tar.gz")
	extractDir := filepath.Join(tmpDir, "extract")
	outsideFile := filepath.Join(tmpDir, "pwned")

	createTestArchive(t, archivePath, []archiveEntry{
		{Name: "../pwned", Content: "owned"},
	})

	err := extractTarGz(archivePath, extractDir)
	if err == nil {
		t.Error("extractTarGz should fail with path traversal attempt")
	}

	if _, err := os.Stat(outsideFile); !os.IsNotExist(err) {
		t.Error("Malicious file was created outside extract dir")
	}
}

func TestExtractTarGzSymlinkSkipped(t *testing.T) {
	tmpDir := t.TempDir()
	archivePath := filepath.Join(tmpDir, "symlink.tar.gz")
	extractDir := filepath.Join(tmpDir, "extract")

	createTestArchive(t, archivePath, []archiveEntry{
		{Name: "evil-link", TypeFlag: tar.TypeSymlink, LinkName: "/etc/passwd"},
		{Name: "normal.txt", Content: "test"},
	})

	if err := extractTarGz(archivePath, extractDir); err != nil {
		t.Fatalf("extractTarGz failed: %v", err)
	}

	if _, err := os.Stat(filepath.Join(extractDir, "normal.txt")); err != nil {
		t.Error("Normal file should have been extracted")
	}

	if _, err := os.Lstat(filepath.Join(extractDir, "evil-link")); !os.IsNotExist(err) {
		t.Error("Symlink should have been skipped")
	}
}

func TestExtractChecksum(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		assetName string
		want      string
	}{
		{
			name:      "standard sha256sum format",
			body:      "abc123def456789012345678901234567890123456789012345678901234abcd  msgvault_darwin_arm64.tar.gz",
			assetName: "msgvault_darwin_arm64.tar.gz",
			want:      "abc123def456789012345678901234567890123456789012345678901234abcd",
		},
		{
			name:      "uppercase checksum",
			body:      "ABC123DEF456789012345678901234567890123456789012345678901234ABCD  msgvault_linux_amd64.tar.gz",
			assetName: "msgvault_linux_amd64.tar.gz",
			want:      "abc123def456789012345678901234567890123456789012345678901234abcd",
		},
		{
			name:      "multiline with target in middle",
			body:      "abc123def456789012345678901234567890123456789012345678901234aaaa  msgvault_linux_amd64.tar.gz\nabc123def456789012345678901234567890123456789012345678901234bbbb  msgvault_darwin_arm64.tar.gz",
			assetName: "msgvault_darwin_arm64.tar.gz",
			want:      "abc123def456789012345678901234567890123456789012345678901234bbbb",
		},
		{
			name:      "no match",
			body:      "abc123def456789012345678901234567890123456789012345678901234abcd  msgvault_linux_amd64.tar.gz",
			assetName: "msgvault_darwin_arm64.tar.gz",
			want:      "",
		},
		{
			name:      "empty body",
			body:      "",
			assetName: "msgvault_darwin_arm64.tar.gz",
			want:      "",
		},
		{
			name:      "substring filename should not match",
			body:      "abc123def456789012345678901234567890123456789012345678901234abcd  msgvault_darwin_arm64.tar.gz.sig",
			assetName: "msgvault_darwin_arm64.tar.gz",
			want:      "",
		},
		{
			name:      "exact match with superset also present",
			body:      "abc123def456789012345678901234567890123456789012345678901234aaaa  msgvault_darwin_arm64.tar.gz.sig\nabc123def456789012345678901234567890123456789012345678901234bbbb  msgvault_darwin_arm64.tar.gz",
			assetName: "msgvault_darwin_arm64.tar.gz",
			want:      "abc123def456789012345678901234567890123456789012345678901234bbbb",
		},
		{
			name:      "binary mode star prefix",
			body:      "abc123def456789012345678901234567890123456789012345678901234abcd *msgvault_darwin_arm64.tar.gz",
			assetName: "msgvault_darwin_arm64.tar.gz",
			want:      "abc123def456789012345678901234567890123456789012345678901234abcd",
		},
		{
			name:      "trailing comment ignored",
			body:      "abc123def456789012345678901234567890123456789012345678901234abcd  msgvault_darwin_arm64.tar.gz  # some comment",
			assetName: "msgvault_darwin_arm64.tar.gz",
			want:      "abc123def456789012345678901234567890123456789012345678901234abcd",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractChecksum(tt.body, tt.assetName)
			if got != tt.want {
				t.Errorf("extractChecksum() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestExtractBaseSemver(t *testing.T) {
	tests := []struct {
		version string
		want    string
	}{
		{"0.1.0", "0.1.0"},
		{"1.2.3", "1.2.3"},
		{"v0.1.0", "0.1.0"},
		{"0.4.0-5-gabcdef", "0.4.0"},
		{"v0.4.0-5-gabcdef", "0.4.0"},
		{"0.4.0-dev", "0.4.0"},
		{"0.4.0-rc1", "0.4.0"},
		{"dev", ""},
		{"abc1234", ""},
		{"88be010", ""},
		{"", ""},
		{"0", ""},
		{"v", ""},
		{"1.0", "1.0"},
	}

	for _, tt := range tests {
		t.Run(tt.version, func(t *testing.T) {
			got := extractBaseSemver(tt.version)
			if got != tt.want {
				t.Errorf("extractBaseSemver(%q) = %q, want %q", tt.version, got, tt.want)
			}
		})
	}
}

func TestIsDevBuildVersion(t *testing.T) {
	tests := []struct {
		version string
		want    bool
	}{
		{"0.1.0", false},
		{"v0.1.0", false},
		{"1.0.0", false},
		{"0.16.1-2-g75d300a", true},
		{"v0.16.1-2-g75d300a", true},
		{"0.4.0-5-gabcdef-dirty", true},
		{"dev", true},
		{"abc1234", true},
		{"0.16.1-rc1", false},
		{"v1.0.0-beta.1", false},
	}

	for _, tt := range tests {
		t.Run(tt.version, func(t *testing.T) {
			got := isDevBuildVersion(tt.version)
			if got != tt.want {
				t.Errorf("isDevBuildVersion(%q) = %v, want %v", tt.version, got, tt.want)
			}
		})
	}
}

func TestIsNewer(t *testing.T) {
	tests := []struct {
		v1, v2 string
		want   bool
	}{
		{"1.0.0", "0.9.0", true},
		{"1.1.0", "1.0.0", true},
		{"1.0.1", "1.0.0", true},
		{"2.0.0", "1.9.9", true},
		{"1.0.0", "1.0.0", false},
		{"0.9.0", "1.0.0", false},
		{"v1.0.0", "v0.9.0", true},
		{"0.4.2", "88be010", false},
		{"0.4.2", "dev", false},
		{"badversion", "0.4.0", false},
		{"0.4.0", "0.4.0-5-gabcdef", false},
		{"0.5.0", "0.4.0-5-gabcdef", true},
		{"0.4.1", "0.4.0-5-gabcdef", true},
		{"0.3.0", "0.4.0-5-gabcdef", false},
		{"0.5.0", "0.4.0-rc1", true},
		{"0.4.0", "0.4.0-rc1", true},  // release > prerelease of same version
		{"0.4.0-rc1", "0.4.0", false}, // prerelease is not newer than release
		{"0.4.0-rc2", "0.4.0-rc1", true},    // rc2 > rc1
		{"0.4.0-rc10", "0.4.0-rc2", true},   // numeric comparison: 10 > 2
		{"0.4.0-rc2", "0.4.0-rc10", false},  // numeric comparison: 2 < 10
		{"0.4.0-beta10", "0.4.0-beta2", true}, // beta10 > beta2
		{"0.4.0-rc1", "0.4.0-beta1", true},  // rc > beta lexicographically
		{"0.4.0-alpha1", "0.4.0-beta1", false}, // alpha < beta
		{"0.4.0-rc.2", "0.4.0-rc.1", true},    // dotted prerelease
		{"0.4.0-1", "0.4.0-rc1", false},       // numeric segment < non-numeric (semver rule)
		{"0.4.0-rc1", "0.4.0-1", true},        // non-numeric > numeric
		{"0.4.0-beta1", "0.3.9", true},        // prerelease of higher base > lower release
	}

	for _, tt := range tests {
		t.Run(tt.v1+"_vs_"+tt.v2, func(t *testing.T) {
			got := isNewer(tt.v1, tt.v2)
			if got != tt.want {
				t.Errorf("isNewer(%q, %q) = %v, want %v", tt.v1, tt.v2, got, tt.want)
			}
		})
	}
}

func TestFindAssets(t *testing.T) {
	assets := []Asset{
		{Name: "msgvault_linux_amd64.tar.gz", Size: 1000, BrowserDownloadURL: "https://example.com/linux_amd64"},
		{Name: "msgvault_darwin_arm64.tar.gz", Size: 2000, BrowserDownloadURL: "https://example.com/darwin_arm64"},
		{Name: "SHA256SUMS", Size: 500, BrowserDownloadURL: "https://example.com/checksums"},
		{Name: "msgvault_darwin_amd64.tar.gz", Size: 3000, BrowserDownloadURL: "https://example.com/darwin_amd64"},
	}

	tests := []struct {
		name             string
		assetName        string
		wantAssetURL     string
		wantAssetSize    int64
		wantChecksumsURL string
		wantAssetNil     bool
	}{
		{
			name:             "find darwin_arm64",
			assetName:        "msgvault_darwin_arm64.tar.gz",
			wantAssetURL:     "https://example.com/darwin_arm64",
			wantAssetSize:    2000,
			wantChecksumsURL: "https://example.com/checksums",
		},
		{
			name:             "find linux_amd64",
			assetName:        "msgvault_linux_amd64.tar.gz",
			wantAssetURL:     "https://example.com/linux_amd64",
			wantAssetSize:    1000,
			wantChecksumsURL: "https://example.com/checksums",
		},
		{
			name:         "asset not found",
			assetName:    "msgvault_freebsd_amd64.tar.gz",
			wantAssetNil: true,
			wantChecksumsURL: "https://example.com/checksums",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			asset, checksums := findAssets(assets, tt.assetName)

			if tt.wantAssetNil {
				if asset != nil {
					t.Errorf("expected asset to be nil, got %+v", asset)
				}
			} else {
				if asset == nil {
					t.Fatal("expected asset to be non-nil")
				}
				if asset.BrowserDownloadURL != tt.wantAssetURL {
					t.Errorf("asset URL = %q, want %q", asset.BrowserDownloadURL, tt.wantAssetURL)
				}
				if asset.Size != tt.wantAssetSize {
					t.Errorf("asset size = %d, want %d", asset.Size, tt.wantAssetSize)
				}
			}

			if checksums == nil {
				t.Fatal("expected checksums to be non-nil")
			}
			if checksums.BrowserDownloadURL != tt.wantChecksumsURL {
				t.Errorf("checksums URL = %q, want %q", checksums.BrowserDownloadURL, tt.wantChecksumsURL)
			}
		})
	}
}

func TestFormatSize(t *testing.T) {
	tests := []struct {
		bytes int64
		want  string
	}{
		{0, "0 B"},
		{100, "100 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1048576, "1.0 MB"},
		{10485760, "10.0 MB"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := FormatSize(tt.bytes)
			if got != tt.want {
				t.Errorf("FormatSize(%d) = %q, want %q", tt.bytes, got, tt.want)
			}
		})
	}
}

// TestSaveCacheFilePermissions verifies that the update check cache file is
// saved with restrictive permissions (0600) to protect user data.
func TestSaveCacheFilePermissions(t *testing.T) {
	// Use a temp directory as MSGVAULT_HOME to avoid touching real user data
	tmpDir := t.TempDir()
	origHome := os.Getenv("MSGVAULT_HOME")
	os.Setenv("MSGVAULT_HOME", tmpDir)
	t.Cleanup(func() {
		if origHome != "" {
			os.Setenv("MSGVAULT_HOME", origHome)
		} else {
			os.Unsetenv("MSGVAULT_HOME")
		}
	})

	// Call saveCache which writes to getCacheDir()/update_check.json
	saveCache("1.0.0")

	cachePath := filepath.Join(tmpDir, cacheFileName)
	info, err := os.Stat(cachePath)
	if err != nil {
		t.Fatalf("Stat(%s) error = %v", cachePath, err)
	}

	// File should have 0600 permissions (owner read/write only)
	got := info.Mode().Perm()
	want := os.FileMode(0600)
	if got != want {
		t.Errorf("cache file permissions = %04o, want %04o", got, want)
	}
}
