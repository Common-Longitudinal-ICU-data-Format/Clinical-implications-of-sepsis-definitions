$ErrorActionPreference = "Stop"

$env:PYTHONUTF8 = "1"
setx PYTHONUTF8 1 | Out-Null

$utf8NoBom = [System.Text.UTF8Encoding]::new($false)
[Console]::InputEncoding = $utf8NoBom
[Console]::OutputEncoding = $utf8NoBom
$OutputEncoding = $utf8NoBom

$logDir = "logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$logFile = Join-Path $logDir "run_$timestamp.log"

Start-Transcript -Path $logFile -Append | Out-Null
try {
    Write-Host "=== Run started: $(Get-Date) ==="

    Write-Host "--- uv sync ---"
    uv sync

    Write-Host "--- 01_cohort.py ---"
    uv run python Code/01_cohort.py

    Write-Host "--- 02_table1.py ---"
    uv run python Code/02_table1.py

    Write-Host "--- 03_ase_visualizations.py ---"
    uv run python Code/03_ase_visualizations.py

    $config = Get-Content -Raw -Path "clif_config.json" | ConvertFrom-Json
    $outputDir = $config.output_directory
    New-Item -ItemType Directory -Force -Path $outputDir | Out-Null
    # Absolute path: quarto resolves --output-dir relative to the .qmd's
    # directory (Code/), so a relative path would land in Code/.
    $outputDir = (Resolve-Path $outputDir).Path

    Write-Host "--- 04_ase_site_analysis_v9.qmd ---"
    quarto render Code/04_ase_site_analysis_v9.qmd --output-dir $outputDir

    Write-Host "=== Run finished: $(Get-Date) ==="
}
finally {
    Stop-Transcript | Out-Null
}
