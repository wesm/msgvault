package update

import (
	"archive/tar"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/wesm/msgvault/internal/testutil"
)

const testHash64 = "abc123def456789012345678901234567890123456789012345678901234abcd"

func TestSanitizeTarPath(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
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

func TestExtractTarGzPathTraversal(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	archivePath := filepath.Join(tmpDir, "malicious.tar.gz")
	extractDir := filepath.Join(tmpDir, "extract")
	outsideFile := filepath.Join(tmpDir, "pwned")

	testutil.CreateTarGz(t, archivePath, []testutil.ArchiveEntry{
		{Name: "../pwned", Content: "owned"},
	})

	err := extractTarGz(archivePath, extractDir)
	if err == nil {
		t.Error("extractTarGz should fail with path traversal attempt")
	}

	testutil.MustNotExist(t, outsideFile)
}

func TestExtractTarGzSymlinkSkipped(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	archivePath := filepath.Join(tmpDir, "symlink.tar.gz")
	extractDir := filepath.Join(tmpDir, "extract")

	testutil.CreateTarGz(t, archivePath, []testutil.ArchiveEntry{
		{Name: "evil-link", TypeFlag: tar.TypeSymlink, LinkName: "/etc/passwd"},
		{Name: "normal.txt", Content: "test"},
	})

	if err := extractTarGz(archivePath, extractDir); err != nil {
		t.Fatalf("extractTarGz failed: %v", err)
	}

	testutil.MustExist(t, filepath.Join(extractDir, "normal.txt"))
	testutil.MustNotExist(t, filepath.Join(extractDir, "evil-link"))
}

func TestExtractChecksum(t *testing.T) {
	t.Parallel()

	hashAAAA := "abc123def456789012345678901234567890123456789012345678901234aaaa"
	hashBBBB := "abc123def456789012345678901234567890123456789012345678901234bbbb"

	tests := []struct {
		name      string
		body      string
		assetName string
		want      string
	}{
		{
			name:      "standard sha256sum format",
			body:      fmt.Sprintf("%s  msgvault_darwin_arm64.tar.gz", testHash64),
			assetName: "msgvault_darwin_arm64.tar.gz",
			want:      testHash64,
		},
		{
			name:      "uppercase checksum",
			body:      "ABC123DEF456789012345678901234567890123456789012345678901234ABCD  msgvault_linux_amd64.tar.gz",
			assetName: "msgvault_linux_amd64.tar.gz",
			want:      testHash64,
		},
		{
			name:      "multiline with target in middle",
			body:      fmt.Sprintf("%s  msgvault_linux_amd64.tar.gz\n%s  msgvault_darwin_arm64.tar.gz", hashAAAA, hashBBBB),
			assetName: "msgvault_darwin_arm64.tar.gz",
			want:      hashBBBB,
		},
		{
			name:      "no match",
			body:      fmt.Sprintf("%s  msgvault_linux_amd64.tar.gz", testHash64),
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
			body:      fmt.Sprintf("%s  msgvault_darwin_arm64.tar.gz.sig", testHash64),
			assetName: "msgvault_darwin_arm64.tar.gz",
			want:      "",
		},
		{
			name:      "exact match with superset also present",
			body:      fmt.Sprintf("%s  msgvault_darwin_arm64.tar.gz.sig\n%s  msgvault_darwin_arm64.tar.gz", hashAAAA, hashBBBB),
			assetName: "msgvault_darwin_arm64.tar.gz",
			want:      hashBBBB,
		},
		{
			name:      "binary mode star prefix",
			body:      fmt.Sprintf("%s *msgvault_darwin_arm64.tar.gz", testHash64),
			assetName: "msgvault_darwin_arm64.tar.gz",
			want:      testHash64,
		},
		{
			name:      "trailing comment ignored",
			body:      fmt.Sprintf("%s  msgvault_darwin_arm64.tar.gz  # some comment", testHash64),
			assetName: "msgvault_darwin_arm64.tar.gz",
			want:      testHash64,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := extractChecksum(tt.body, tt.assetName)
			if got != tt.want {
				t.Errorf("extractChecksum() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestExtractBaseSemver(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
			got := extractBaseSemver(tt.version)
			if got != tt.want {
				t.Errorf("extractBaseSemver(%q) = %q, want %q", tt.version, got, tt.want)
			}
		})
	}
}

func TestIsDevBuildVersion(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
			got := isDevBuildVersion(tt.version)
			if got != tt.want {
				t.Errorf("isDevBuildVersion(%q) = %v, want %v", tt.version, got, tt.want)
			}
		})
	}
}

func TestIsNewer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		v1, v2 string
		want   bool
	}{
		{"major version bump", "1.0.0", "0.9.0", true},
		{"minor version bump", "1.1.0", "1.0.0", true},
		{"patch version bump", "1.0.1", "1.0.0", true},
		{"major boundary crossing", "2.0.0", "1.9.9", true},
		{"same version not newer", "1.0.0", "1.0.0", false},
		{"older version not newer", "0.9.0", "1.0.0", false},
		{"v prefix handled", "v1.0.0", "v0.9.0", true},
		{"release vs non-semver hash", "0.4.2", "88be010", false},
		{"release vs dev string", "0.4.2", "dev", false},
		{"bad version not newer", "badversion", "0.4.0", false},
		{"same base as dev build not newer", "0.4.0", "0.4.0-5-gabcdef", false},
		{"higher minor than dev build", "0.5.0", "0.4.0-5-gabcdef", true},
		{"higher patch than dev build", "0.4.1", "0.4.0-5-gabcdef", true},
		{"lower version than dev build", "0.3.0", "0.4.0-5-gabcdef", false},
		{"higher minor than prerelease", "0.5.0", "0.4.0-rc1", true},
		{"release newer than its prerelease", "0.4.0", "0.4.0-rc1", true},
		{"prerelease not newer than release", "0.4.0-rc1", "0.4.0", false},
		{"rc2 newer than rc1", "0.4.0-rc2", "0.4.0-rc1", true},
		{"numeric prerelease comparison rc10 vs rc2", "0.4.0-rc10", "0.4.0-rc2", true},
		{"numeric prerelease comparison rc2 vs rc10", "0.4.0-rc2", "0.4.0-rc10", false},
		{"numeric prerelease beta10 vs beta2", "0.4.0-beta10", "0.4.0-beta2", true},
		{"rc newer than beta lexicographically", "0.4.0-rc1", "0.4.0-beta1", true},
		{"alpha older than beta", "0.4.0-alpha1", "0.4.0-beta1", false},
		{"dotted prerelease comparison", "0.4.0-rc.2", "0.4.0-rc.1", true},
		{"numeric segment less than non-numeric", "0.4.0-1", "0.4.0-rc1", false},
		{"non-numeric greater than numeric", "0.4.0-rc1", "0.4.0-1", true},
		{"prerelease of higher base beats lower release", "0.4.0-beta1", "0.3.9", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := isNewer(tt.v1, tt.v2)
			if got != tt.want {
				t.Errorf("isNewer(%q, %q) = %v, want %v", tt.v1, tt.v2, got, tt.want)
			}
		})
	}
}

func TestFindAssets(t *testing.T) {
	t.Parallel()
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
			name:             "asset not found",
			assetName:        "msgvault_freebsd_amd64.tar.gz",
			wantAssetNil:     true,
			wantChecksumsURL: "https://example.com/checksums",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
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
	t.Parallel()
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
			t.Parallel()
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
	if runtime.GOOS == "windows" {
		t.Skip("POSIX file permissions not enforced on Windows")
	}

	// Use a temp directory as MSGVAULT_HOME to avoid touching real user data
	tmpDir := t.TempDir()
	t.Setenv("MSGVAULT_HOME", tmpDir)

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
