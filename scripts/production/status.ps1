# Read-only status: counts, jobs, leases, queue, plus jobs waiting for retry and open dead letters.
# Usage: .\scripts\production\status.ps1
. "$PSScriptRoot\_common.ps1"
Assert-Venv
[void](Import-DotEnv)
if (-not $env:KB_DATABASE_URL) { throw 'KB_DATABASE_URL is not set (.env or environment).' }
$db = Get-ExpectedDatabase
$code = Invoke-Pipeline 'app.pipeline.cli' @('status', '--expect-database', $db)
Write-Host "`n--- jobs waiting for retry ---"
[void](Invoke-Pipeline 'app.pipeline.cli' @('jobs', '--expect-database', $db, '--status', 'retry_pending', '--limit', '20'))
Write-Host "`n--- open dead letters ---"
[void](Invoke-Pipeline 'app.pipeline.cli' @('dead-letters', '--expect-database', $db, '--limit', '20'))
exit $code
