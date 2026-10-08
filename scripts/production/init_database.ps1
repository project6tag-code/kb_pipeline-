# Initialize an EMPTY PostgreSQL 17 + pgvector database for the pipeline, from repository scripts only.
# Run ONCE per database, as an operator who knows the PostgreSQL administrator password.
#
#   .\scripts\production\init_database.ps1 -Database kb_prod
#   .\scripts\production\init_database.ps1 -Database kb_cc_smoke -CopyPgPassFrom kb_pipeline_test   # disposable test DB
#
# Steps (each idempotent): create database -> vector extension -> public tables (scripts\db\bootstrap_public_tables.sql)
# -> roles and grants (provision_kbp_roles.sql) -> Alembic (schema kbp) -> frozen registrations -> Gold bridge
# -> Gold V2 patch -> map bootstrap -> model 2 promotion. Reuses the existing CLIs and SQL; no schema is defined here.
#
# Secrets: the administrator password is read with a secure prompt and handed to child processes only through the
# PGPASSWORD environment variable, which is removed at the end. The pipeline roles' passwords come from pgpass.conf.
param(
    [Parameter(Mandatory = $true)][ValidatePattern('^[a-z][a-z0-9_]{0,62}$')][string]$Database,
    [string]$PgBin = 'C:\Program Files\PostgreSQL\17\bin',
    [string]$AdminUser = 'postgres',
    [string]$HostName = 'localhost',
    [int]$Port = 5432,
    [string]$CopyPgPassFrom = '',
    [switch]$SkipModelDownload
)
. "$PSScriptRoot\_common.ps1"
Assert-Venv
if ($Database -in 'postgres', 'template0', 'template1', 'data_collection_pipeline') { throw "Refusing protected database '$Database'." }
Set-Location $script:Root

$psql = Join-Path $PgBin 'psql.exe'
$createdb = Join-Path $PgBin 'createdb.exe'
foreach ($tool in $psql, $createdb) { if (-not (Test-Path $tool)) { throw "Missing $tool (use -PgBin)." } }

function Step([string]$Name) { Write-Host "`n==> $Name" -ForegroundColor Cyan }
function Check([string]$What, [int]$Code) { if ($Code -ne 0) { throw "$What failed (exit $Code)." } }
function Psql([string[]]$Arguments) {
    & $psql -X -v ON_ERROR_STOP=1 -h $HostName -p $Port -U $AdminUser @Arguments | Out-Host
    return [int]$LASTEXITCODE
}
function Py([string]$Module, [string[]]$Arguments) { return (Invoke-Pipeline $Module $Arguments) }

$secure = Read-Host "PostgreSQL password for '$AdminUser' (not stored)" -AsSecureString
$bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
try {
    $env:PGPASSWORD = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
    $env:KB_EXPECTED_DATABASE = $Database
    $base = "postgresql+psycopg://{0}@${HostName}:$Port/$Database"
    $env:KB_DATABASE_URL = $base -f 'kb_pipeline_app'
    $env:KB_MIGRATION_DATABASE_URL = $base -f 'kb_pipeline_migrator'
    $env:KB_ADMIN_DATABASE_URL = $base -f $AdminUser

    Step "Create database $Database (UTF8) if missing"
    $exists = (& $psql -X -tA -h $HostName -p $Port -U $AdminUser -d postgres -c "SELECT 1 FROM pg_database WHERE datname = '$Database'")
    if ($LASTEXITCODE -ne 0) { throw 'Cannot connect as the administrator (wrong password?).' }
    if (-not $exists) { & $createdb -h $HostName -p $Port -U $AdminUser -E UTF8 -T template0 $Database; Check 'createdb' $LASTEXITCODE }
    else { Write-Host "database $Database already exists" }

    Step 'pgvector extension'
    Check 'CREATE EXTENSION vector' (Psql @('-d', $Database, '-c', 'CREATE EXTENSION IF NOT EXISTS vector;'))

    Step 'Public tables (kb, metadata, datasets, dataset_sources)'
    Check 'bootstrap_public_tables.sql' (Psql @('-d', $Database, '-f', 'scripts\db\bootstrap_public_tables.sql'))

    Step 'Roles, schema kbp and grants'
    # Provisioning is a once-only step: after the Gold V2 patch grants INSERT on public.datasets, its own self-check
    # would refuse a second run. Schema kbp existing means it already ran, so a rerun of this script skips it.
    $provisioned = (& $psql -X -tA -h $HostName -p $Port -U $AdminUser -d $Database -c "SELECT 1 FROM pg_namespace WHERE nspname = 'kbp'")
    if ($LASTEXITCODE -ne 0) { throw 'Cannot query the new database.' }
    if ($provisioned) { Write-Host 'schema kbp already exists: provisioning was done earlier, skipped' }
    else { Check 'provision_kbp_roles.sql' (Psql @('-d', $Database, '-v', "target_db=$Database", '-f', 'scripts\db\provision_kbp_roles.sql')) }

    Step 'Pipeline role passwords (pgpass.conf)'
    $pgpass = Join-Path $env:APPDATA 'postgresql\pgpass.conf'
    foreach ($role in 'kb_pipeline_app', 'kb_pipeline_migrator') {
        $have = (Test-Path $pgpass) -and (Select-String -Path $pgpass -Pattern "^[^:]*:\d+:(\*|${Database}):${role}:" -Quiet)
        if (-not $have -and $CopyPgPassFrom -and (Test-Path $pgpass)) {
            $src = Select-String -Path $pgpass -Pattern "^([^:]*):(\d+):${CopyPgPassFrom}:${role}:(.*)$" | Select-Object -First 1
            if ($src) {
                Add-Content -Path $pgpass -Value ("{0}:{1}:{2}:{3}:{4}" -f $src.Matches[0].Groups[1].Value, $src.Matches[0].Groups[2].Value, $Database, $role, $src.Matches[0].Groups[3].Value)
                $have = $true
                Write-Host "pgpass: copied the $role entry of '$CopyPgPassFrom' to '$Database'"
            }
        }
        if (-not $have) { throw "No pgpass.conf entry for $role on database $Database. Set the role password (psql: \password $role) and add '${HostName}:${Port}:${Database}:${role}:<password>' to $pgpass, then rerun." }
    }

    Step 'Alembic: schema kbp'
    Check 'alembic upgrade head' (Py 'alembic' @('-x', "expect_database=$Database", 'upgrade', 'head'))

    Step 'Embedding model id 2 (the runtime pins MODEL_ID = 2; see scripts\db\ensure_embedding_model_id.sql)'
    Check 'ensure_embedding_model_id.sql' (Psql @('-d', $Database, '-v', "expected_database=$Database", '-f', 'scripts\db\ensure_embedding_model_id.sql'))

    Step 'Frozen registrations (migrator)'
    Check 'silver register-methods' (Py 'app.processing.silver.cli' @('register-methods', '--expect-database', $Database))
    Check 'chunking register-version' (Py 'app.processing.chunking.cli' @('register-version', '--expect-database', $Database))
    Check 'chunk_dedup register-method' (Py 'app.processing.chunk_dedup.cli' @('register-method', '--expect-database', $Database))
    if (-not $SkipModelDownload) { Check 'embedding download' (Py 'app.processing.embedding.cli' @('download')) }
    Check 'embedding register-model' (Py 'app.processing.embedding.cli' @('register-model', '--expect-database', $Database))

    Step 'Gold bridge rows (administrator), Gold V2 grants, bridge map'
    Check 'gold_bridge admin-bootstrap' (Py 'app.processing.gold_bridge.cli' @('admin-bootstrap', '--expect-database', $Database))
    Check 'admin_gold_v2_patch.sql' (Psql @('-d', $Database, '-v', "expected_database=$Database", '-f', 'scripts\db\admin_gold_v2_patch.sql'))
    Check 'gold_bridge map-bootstrap' (Py 'app.processing.gold_bridge.cli' @('map-bootstrap', '--expect-database', $Database))

    Step 'Promote embedding model 2 to active_gold'
    Check 'gold promote-model' (Py 'app.processing.gold.cli' @('promote-model', '--expect-database', $Database))

    Step 'Final status (runtime role)'
    Check 'status' (Py 'app.pipeline.cli' @('status', '--expect-database', $Database))
    Write-Host "`nDatabase '$Database' is initialized. Put KB_EXPECTED_DATABASE=$Database in .env, then run check_setup.ps1." -ForegroundColor Green
} finally {
    Remove-Item Env:\PGPASSWORD -ErrorAction SilentlyContinue
    Remove-Item Env:\KB_ADMIN_DATABASE_URL -ErrorAction SilentlyContinue
    if ($bstr -ne [IntPtr]::Zero) { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
}
