# Build the clean deployment ZIP and its manifest (dist\). Packages only tracked-style source/config/DB scripts/docs;
# never .venv, .env, data, logs, backups, dumps, caches or secrets. Nothing is committed or modified.
# Usage: .\scripts\production\build_release.ps1 [-Version v2-cc]
param([string]$Version = 'cc-v2')
. "$PSScriptRoot\_common.ps1"
Set-Location $script:Root
Add-Type -AssemblyName System.IO.Compression
Add-Type -AssemblyName System.IO.Compression.FileSystem

$include = @('app', 'migrations', 'scripts', 'docs', 'tests', 'artifacts/db', 'config')
$rootFiles = @('pyproject.toml', 'alembic.ini', 'requirements-embeddings.txt', '.env.example', '.gitignore',
    'README.md', 'README_PRODUCTION.md')
$placeholders = @('data/bronze/commoncrawl/.gitkeep', 'data/silver/.gitkeep', 'data/quarantine/.gitkeep', 'logs/.gitkeep')
$excludeRegex = '(^|/)(__pycache__|\.pytest_cache|\.mypy_cache|\.ruff_cache|\.cache|\.venv|venv|\.git|tmp|temp)(/|$)|' +
    '\.(pyc|pyo|log|dump|backup|bak|tmp|sqlite3|sqlite3-wal|sqlite3-shm|pem|key|pfx|zip)$|(^|/)\.env$|(^|/)pgpass\.conf$|\.egg-info(/|$)'

function RelPath([string]$full) { $full.Substring($script:Root.Length + 1).Replace('\', '/') }

$files = @()
foreach ($dir in $include) {
    if (Test-Path $dir) { $files += Get-ChildItem $dir -Recurse -File -Force | ForEach-Object { RelPath $_.FullName } }
}
foreach ($f in $rootFiles + $placeholders) { if (Test-Path $f) { $files += $f } }
$files = $files | Where-Object { $_ -notmatch $excludeRegex } | Sort-Object -Unique
$files = $files | Where-Object { (Get-Item $_).Length -gt 0 -or $_ -like '*.gitkeep' -or $_ -like '*__init__.py' }

$commit = (git rev-parse --short HEAD 2>$null)
$dirty = [bool](git status --porcelain 2>$null)
$stamp = Get-Date -Format 'yyyyMMdd-HHmm'
$dist = Join-Path $script:Root 'dist'
New-Item -ItemType Directory -Force -Path $dist | Out-Null
$zipPath = Join-Path $dist "kb-pipeline-$Version-$stamp.zip"
$manifestPath = Join-Path $dist "DEPLOYMENT_MANIFEST-$Version-$stamp.md"

$lines = @("# Deployment manifest: kb-pipeline $Version", '',
    "- Built: $(Get-Date -Format 'yyyy-MM-dd HH:mm') on $env:COMPUTERNAME",
    "- Git base: $commit$(if ($dirty) { ' + UNCOMMITTED changes (not yet committed)' })",
    "- Files: $($files.Count)", '', '## Included (path, bytes, sha256)', '')
foreach ($f in $files) {
    $item = Get-Item $f
    $lines += ('- `{0}` ({1}) {2}' -f $f, $item.Length, (Get-FileHash $f -Algorithm SHA256).Hash.Substring(0, 16).ToLower())
}
$lines += @('', '## Deliberately excluded (local-only, never deployed)', '',
    '- `.venv/`, `venv/`: recreate with `python -m venv` + `pip install` (README_PRODUCTION.md)',
    '- `.env`, `pgpass.conf`, `*.pem`, `*.key`, `*.pfx`: secrets; create on the target machine',
    '- `data/bronze/**`, `data/silver/*`, `data/chunk_dedup/*`, `data/quarantine/*`: runtime data and rebuildable indexes',
    '- `logs/*`, `*.log`: runtime logs',
    '- `backup/`, `*.dump`, `*.backup`, `*.bak`, `*.sql.gz`: database backups',
    '- `*.sqlite3*`, `__pycache__/`, `*.pyc`, `.pytest_cache/`, `.cache/`, `*.egg-info/`, `tmp/`, `temp/`: caches and temp files',
    '- `.git/`, `dist/`, previous `*.zip`: repository metadata and build output',
    '- The Hugging Face model cache (~1.1 GB): fetched on the target with `embedding.cli download`',
    '', '## Required on the target machine (not in the ZIP)', '',
    '- Windows 10/11, Python 3.12, PostgreSQL 17 with the pgvector extension, optional NVIDIA GPU',
    '- A `.env` created from `.env.example`, a `pgpass.conf` holding the role passwords',
    '- The four `public` tables (`kb`, `metadata`, `datasets`, `dataset_sources`): see README_PRODUCTION.md section 4')
$lines | Set-Content -Path $manifestPath -Encoding UTF8

if (Test-Path $zipPath) { Remove-Item $zipPath -Force }
$zip = [System.IO.Compression.ZipFile]::Open($zipPath, 'Create')
try {
    foreach ($f in $files) {
        [void][System.IO.Compression.ZipFileExtensions]::CreateEntryFromFile($zip, (Join-Path $script:Root $f), $f, 'Optimal')
    }
    [void][System.IO.Compression.ZipFileExtensions]::CreateEntryFromFile($zip, $manifestPath, 'DEPLOYMENT_MANIFEST.md', 'Optimal')
} finally { $zip.Dispose() }

# Safety scan of the finished ZIP
$zip = [System.IO.Compression.ZipFile]::OpenRead($zipPath)
$bad = $zip.Entries | Where-Object { $_.FullName -match $excludeRegex.Replace('\.(pyc|pyo', '\.(pyc|pyo') -and $_.FullName -ne 'DEPLOYMENT_MANIFEST.md' }
$count = $zip.Entries.Count
$zip.Dispose()
if ($bad) { Remove-Item $zipPath -Force; throw "ZIP contained excluded files: $($bad.FullName -join ', ')" }
Write-Host "ZIP:      $zipPath ($count entries, $([math]::Round((Get-Item $zipPath).Length / 1MB, 2)) MB)"
Write-Host "Manifest: $manifestPath"
