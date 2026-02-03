# Convert .doc files to HTML using Microsoft Word
# Usage: powershell -ExecutionPolicy Bypass -File convert_to_html.ps1 [-Path <folder>] [-Limit <n>]

param(
    [string]$Path = "E:\Google Drive\futureheist\city\water-quality\downloads",
    [int]$Limit = 0,
    [switch]$Force
)

$ErrorActionPreference = "Stop"

# Start Word application
Write-Host "Starting Microsoft Word..."
$word = New-Object -ComObject Word.Application
$word.Visible = $false
$word.DisplayAlerts = 0  # wdAlertsNone

$converted = 0
$skipped = 0
$failed = 0

try {
    # Find all .doc files
    $docFiles = Get-ChildItem -Path $Path -Filter "*.doc" -Recurse | Where-Object { $_.Extension -eq ".doc" }

    if ($Limit -gt 0) {
        $docFiles = $docFiles | Select-Object -First $Limit
    }

    $total = $docFiles.Count
    Write-Host "Found $total .doc files to process"

    foreach ($docFile in $docFiles) {
        $htmlPath = $docFile.FullName -replace '\.doc$', '.html'

        # Skip if HTML already exists (unless -Force)
        if ((Test-Path $htmlPath) -and -not $Force) {
            $skipped++
            continue
        }

        try {
            Write-Host "Converting: $($docFile.Name)" -NoNewline

            # Open document
            $doc = $word.Documents.Open($docFile.FullName, $false, $true)  # ReadOnly

            # Save as filtered HTML (cleaner than full HTML)
            # wdFormatFilteredHTML = 10
            $doc.SaveAs([ref]$htmlPath, [ref]10)

            $doc.Close($false)  # Don't save changes

            $converted++
            Write-Host " -> Done" -ForegroundColor Green
        }
        catch {
            $failed++
            Write-Host " -> FAILED: $($_.Exception.Message)" -ForegroundColor Red
        }

        # Progress update every 100 files
        if (($converted + $skipped + $failed) % 100 -eq 0) {
            Write-Host "Progress: $converted converted, $skipped skipped, $failed failed"
        }
    }
}
finally {
    # Clean up Word
    Write-Host "Closing Word..."
    $word.Quit()
    [System.Runtime.Interopservices.Marshal]::ReleaseComObject($word) | Out-Null
    [System.GC]::Collect()
    [System.GC]::WaitForPendingFinalizers()
}

Write-Host ""
Write-Host "=== Summary ==="
Write-Host "Converted: $converted"
Write-Host "Skipped: $skipped"
Write-Host "Failed: $failed"
