# Run the Common Crawl pipeline with safe initial limits: one acquisition pass, then --skip-acquisition passes
# until every stage (dedup, embedding, Gold) completes. Every pass is the same idempotent `app.pipeline.cli run`.
# Usage:
#   .\scripts\production\run_cc.ps1 -Crawl CC-MAIN-2024-33                  # reference publishers, 2 partitions
#   .\scripts\production\run_cc.ps1 -Crawl CC-MAIN-2024-33 -DryRun          # print the stage plan only
#   .\scripts\production\run_cc.ps1 -Crawl CC-MAIN-2024-33 -Profile both -Partitions 4
# Stop with Ctrl+C ONCE (workers finish the batch). Re-run the same command to resume.
param(
    [Parameter(Mandatory = $true)][ValidatePattern('^CC-MAIN-\d{4}-\d{2}$')][string]$Crawl,
    [ValidateSet('reference', 'open', 'both')][string]$Profile = 'reference',
    [ValidateRange(1, 200)][int]$Partitions = 2,
    [ValidateRange(1, 10)][int]$Blocks = 1,
    [ValidateRange(1, 100)][int]$Iterations = 4,
    [ValidateRange(1, 20)][int]$MaxPasses = 8,
    [string]$Device = 'auto',
    [switch]$DryRun
)
. "$PSScriptRoot\_common.ps1"
Assert-Venv
[void](Import-DotEnv)
if (-not $env:KB_DATABASE_URL) { throw 'KB_DATABASE_URL is not set (.env or environment).' }

if ($env:KB_ENABLE_GITHUB -match '^(1|true|yes)$') { throw 'KB_ENABLE_GITHUB is set: this release is Common Crawl only. Unset it.' }
$common = @('run', '--expect-database', (Get-ExpectedDatabase), '--sources', 'cc', '--device', $Device)
if ($DryRun) {
    $code = Invoke-Pipeline 'app.pipeline.cli' ($common + @('--cc-profile', $Profile, '--cc-crawl', $Crawl,
        '--cc-partitions', "$Partitions", '--cc-blocks', "$Blocks", '--cc-iterations', "$Iterations", '--dry-run'))
    exit $code
}

$first = $common + @('--cc-profile', $Profile, '--cc-crawl', $Crawl, '--cc-partitions', "$Partitions",
    '--cc-blocks', "$Blocks", '--cc-iterations', "$Iterations")
Write-Host "Pass 0: acquisition + processing ($Crawl, profile $Profile, $Partitions partition(s) x $Blocks block(s))"
$code = Invoke-Pipeline 'app.pipeline.cli' $first
for ($pass = 1; $code -ne 0 -and $pass -le $MaxPasses; $pass++) {
    if ($code -eq 2 -or $code -eq 130) { break }   # refused / interrupted: do not loop
    Write-Host "Pass ${pass}: --skip-acquisition (previous exit $code; each pass embeds up to ~10,000 chunks)"
    $code = Invoke-Pipeline 'app.pipeline.cli' ($common + @('--skip-acquisition'))
}
if ($code -eq 0) { Write-Host 'Run complete: Gold published.' } else { Write-Host "Run stopped with exit code $code (see logs\pipeline\<run_id>\)." }
exit $code
