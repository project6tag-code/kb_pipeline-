# Pre-deployment validation (offline, no database needed): required files, secrets hygiene, .gitignore coverage,
# migration safety checker, and the unit tests. Known unrelated GitHub tests (test_gh_*) are not run.
# Usage: .\scripts\production\validate_predeploy.ps1        (exit 0 = ready to package)
. "$PSScriptRoot\_common.ps1"
Assert-Venv
$failed = 0
function Report([string]$Name, [bool]$Ok, [string]$Detail) {
    $mark = if ($Ok) { 'OK  ' } else { 'FAIL' }
    Write-Host ("[{0}] {1} {2}" -f $mark, $Name, $Detail)
    if (-not $Ok) { $script:failed++ }
}
Set-Location $script:Root

$required = @('pyproject.toml', 'alembic.ini', 'requirements-embeddings.txt', '.env.example', '.gitignore',
    'README_PRODUCTION.md', 'migrations/env.py', 'app/pipeline/cli.py', 'scripts/db/provision_kbp_roles.sql',
    'scripts/db/admin_gold_v2_patch.sql', 'scripts/db/bootstrap_public_tables.sql', 'scripts/db/ensure_embedding_model_id.sql', 'scripts/production/init_database.ps1',
    'app/config/target.py', 'app/config/features.py', 'scripts/db/admin_reset_kb_pipeline_test.sql',
    'app/sources/commoncrawl/reference_domains_v1.json', 'app/processing/classification/taxonomy_v1.json') +
    (Get-ChildItem migrations/versions -Filter 'kbp_*.py' | ForEach-Object { "migrations/versions/$($_.Name)" })
$missing = $required | Where-Object { -not (Test-Path $_) }
Report 'Required files present' (-not $missing) ("(missing: " + ($missing -join ', ') + ")")

$ignore = Get-Content .gitignore -Raw
$patterns = '.env', '.venv/', 'backup/', '*.dump', 'logs/*', 'data/bronze', 'data/silver/*', '__pycache__/', 'pgpass.conf'
$notIgnored = $patterns | Where-Object { $ignore -notlike "*$_*" }
Report '.gitignore covers secrets, backups, runtime data' (-not $notIgnored) ("(missing: " + ($notIgnored -join ', ') + ")")
function Test-GitIgnored([string]$Path) {
    git check-ignore -q $Path 2>$null
    return ($LASTEXITCODE -eq 0)
}
$overIgnored = @('scripts/db/provision_kbp_roles.sql', 'migrations/versions/kbp_001a_schema_v1.py', 'app/pipeline/cli.py',
    '.env.example', 'README_PRODUCTION.md', 'scripts/production/run_cc.ps1') | Where-Object { Test-GitIgnored $_ }
Report 'Source, migrations, DB scripts and .env.example are not ignored' (-not $overIgnored) "(ignored: $($overIgnored -join ', '))"

$example = Get-Content .env.example -Raw
$realPasswords = [regex]::Matches($example, '://[^@/\s:]+:([^@/\s]+)@') | Where-Object { $_.Groups[1].Value -ne 'CHANGE_ME' }
Report '.env.example has only CHANGE_ME placeholders, no real passwords or tokens' ((-not $realPasswords) -and ($example -notmatch 'ghp_|github_pat_|AKIA')) ''
$tracked = (git ls-files 2>$null) -join "`n"
Report 'No .env / dump / data / backup file is tracked' ($tracked -notmatch '(^|\n)(\.env|backup/|data/bronze/.+|.*\.dump)(\n|$)') ''

$code = Invoke-Pipeline 'app.db.migration_safety' @()
Report 'Migration safety checker' ($code -eq 0) "(exit $code)"

$modules = Get-ChildItem tests/unit -Filter 'test_*.py' | Where-Object { $_.Name -notlike 'test_gh_*' } |
    ForEach-Object { 'tests.unit.' + $_.BaseName }
$previousExpectedDatabase = $env:KB_EXPECTED_DATABASE

try {
    # Unit tests are written against the isolated test database name.
    $env:KB_EXPECTED_DATABASE = 'kb_pipeline_test'

    $code = Invoke-Pipeline 'unittest' $modules
}
finally {
    if ($null -eq $previousExpectedDatabase) {
        Remove-Item Env:\KB_EXPECTED_DATABASE -ErrorAction SilentlyContinue
    }
    else {
        $env:KB_EXPECTED_DATABASE = $previousExpectedDatabase
    }
}

Report 'Unit tests (GitHub tests excluded)' ($code -eq 0) "(exit $code)"

if ($failed) { Write-Host "`n$failed check(s) failed."; exit 1 }
Write-Host "`nPre-deployment validation OK."
