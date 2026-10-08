# Setup check: Python/venv, packages, .env, secrets hygiene, disk space, GPU, database target/role/revision/model.
# Usage: .\scripts\production\check_setup.ps1        (exit 0 = ready, 1 = something to fix)
. "$PSScriptRoot\_common.ps1"
$failed = 0
function Report([string]$Name, [bool]$Ok, [string]$Detail) {
    $mark = if ($Ok) { 'OK  ' } else { 'FAIL' }
    Write-Host ("[{0}] {1} {2}" -f $mark, $Name, $Detail)
    if (-not $Ok) { $script:failed++ }
}

Report '.env present' (Import-DotEnv) '(copy .env.example to .env and edit)'
Assert-Venv
$version = & $script:Py -c "import sys; print('%d.%d' % sys.version_info[:2])"
Report 'Python 3.12 in .venv' ($version -eq '3.12') "(found $version)"

$packages = & $script:Py -c "
import importlib
missing = [m for m in ('psycopg', 'sqlalchemy', 'alembic', 'torch', 'sentence_transformers', 'numpy') if importlib.util.find_spec(m) is None]
print(','.join(missing))"
Report 'Python packages (pip install -e . and requirements-embeddings.txt)' (-not $packages) "(missing: $packages)"

foreach ($name in 'KB_DATABASE_URL', 'KB_MIGRATION_DATABASE_URL') {
    $value = [Environment]::GetEnvironmentVariable($name, 'Process')
    Report "$name set" ([bool]$value) ''
    if ($value) { Report "$name has no inline password or CHANGE_ME placeholder" ($value -notmatch '://[^@/]*:[^@/]+@') '(remove ":password"; use pgpass.conf)' }
}
$expectedDb = Get-ExpectedDatabase
Report 'KB_DATABASE_URL names the expected database (KB_EXPECTED_DATABASE)' ($env:KB_DATABASE_URL -match "/$expectedDb(\?|$)") "(expected /$expectedDb)"
Report 'GitHub ingestion is disabled' (-not ($env:KB_ENABLE_GITHUB -match '^(1|true|yes)$')) '(Common Crawl only; unset KB_ENABLE_GITHUB)'

$drive = Get-PSDrive -Name ($script:Root.Substring(0, 1))
$freeGb = [math]::Round($drive.Free / 1GB, 1)
Report 'Free disk space >= 20 GB' ($freeGb -ge 20) "($freeGb GB free)"

$cuda = & $script:Py -c "import torch; print(torch.cuda.is_available())" 2>$null
Write-Host "[INFO] CUDA available: $cuda (embedding falls back to CPU: slower, vector bits are device-specific)"

# Database target, role, schema revision and model status come from the existing status command.
$code = Invoke-Pipeline 'app.pipeline.cli' @('status', '--expect-database', $expectedDb)
Report 'Database status (target, role kb_pipeline_app, revision kbp_001e, model 2 active_gold)' ($code -eq 0) "(exit $code)"

if ($failed) { Write-Host "`n$failed check(s) failed."; exit 1 }
Write-Host "`nSetup OK."
