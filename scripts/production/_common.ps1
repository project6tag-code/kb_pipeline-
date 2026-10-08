# Shared helpers for the production operator scripts (dot-sourced; no pipeline logic lives here).
# Windows PowerShell 5.1 compatible.

$ErrorActionPreference = 'Stop'
$script:Root = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$script:Py = Join-Path $script:Root '.venv\Scripts\python.exe'
function Get-ExpectedDatabase {
    # The database every command must reach: KB_EXPECTED_DATABASE (.env or environment), default kb_pipeline_test.
    # The pipeline itself re-checks it against --expect-database and the live connection.
    if ($env:KB_EXPECTED_DATABASE) { return $env:KB_EXPECTED_DATABASE.Trim() }
    return 'kb_pipeline_test'
}

function Import-DotEnv {
    # Load KEY=VALUE lines of .env into the process environment. Real environment variables win.
    $file = Join-Path $script:Root '.env'
    if (-not (Test-Path $file)) { return $false }
    foreach ($line in Get-Content $file -Encoding UTF8) {
        $text = $line.Trim()
        if (-not $text -or $text.StartsWith('#')) { continue }
        $i = $text.IndexOf('=')
        if ($i -lt 1) { continue }
        $name = $text.Substring(0, $i).Trim()
        $value = $text.Substring($i + 1).Trim().Trim('"').Trim("'")
        if (-not [Environment]::GetEnvironmentVariable($name, 'Process')) {
            [Environment]::SetEnvironmentVariable($name, $value, 'Process')
        }
    }
    return $true
}

function Assert-Venv {
    if (-not (Test-Path $script:Py)) { throw "Missing $script:Py. Create the venv first (README_PRODUCTION.md, section 2)." }
}

function Invoke-Pipeline {
    # Run `python -m <module> <args>` from the repository root; returns the exit code.
    param([string]$Module, [string[]]$Arguments)
    Push-Location $script:Root
    try {
        & $script:Py -m $Module @Arguments | Out-Host   # show output; the function returns only the exit code
        return [int]$LASTEXITCODE
    } finally { Pop-Location }
}
