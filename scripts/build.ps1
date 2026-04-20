# Dev build script for Windows
# Usage: powershell -File scripts/build.ps1
#
# Builds msgvault.exe in the repo root with debug info and FTS5 +
# sqlite-vec support. Requires Go, a C compiler (GCC via MSYS2/MinGW
# or TDM-GCC), and sqlite3 development headers. Install them under
# MSYS2 with `pacman -S mingw-w64-x86_64-sqlite3` and ensure
# CGO_CFLAGS points to the include directory (this script sets it
# automatically for the default MSYS2 path).

$ErrorActionPreference = 'Stop'

$version = & git describe --tags --always --dirty 2>$null
if (-not $version) { $version = "dev" }
$commit = & git rev-parse --short HEAD 2>$null
if (-not $commit) { $commit = "unknown" }
$buildDate = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")

$ldflags = @(
    "-X github.com/wesm/msgvault/cmd/msgvault/cmd.Version=$version"
    "-X github.com/wesm/msgvault/cmd/msgvault/cmd.Commit=$commit"
    "-X github.com/wesm/msgvault/cmd/msgvault/cmd.BuildDate=$buildDate"
) -join ' '

$env:CGO_ENABLED = 1
if (-not $env:CGO_CFLAGS -and (Test-Path "C:\msys64\mingw64\include\sqlite3.h")) {
    $env:CGO_CFLAGS = "-IC:/msys64/mingw64/include"
}

Write-Host "Building msgvault $version ($commit)..."
& go build -tags "fts5 sqlite_vec" -ldflags "$ldflags" -o msgvault.exe ./cmd/msgvault
if ($LASTEXITCODE -ne 0) {
    Write-Host "Build failed." -ForegroundColor Red
    exit 1
}
Write-Host "Built: msgvault.exe" -ForegroundColor Green
