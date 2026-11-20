[CmdletBinding()]
param(
    [string]$MetaEditorPath = $env:MQL5_METAEDITOR_PATH
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($MetaEditorPath)) {
    throw "MQL5_METAEDITOR_PATH is not configured. Install MetaTrader 5 on the self-hosted runner and set this variable to metaeditor64.exe."
}

if (-not (Test-Path -LiteralPath $MetaEditorPath -PathType Leaf)) {
    throw "MetaEditor executable was not found at: $MetaEditorPath"
}

$repositoryRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$expertFiles = @(
    "mt5/Experts/GoldBotSessionBridge.mq5",
    "mt5/Experts/GoldBotClockProbe.mq5"
)

$logDirectory = Join-Path $env:RUNNER_TEMP "goldbot-mql5-compile"
New-Item -ItemType Directory -Path $logDirectory -Force | Out-Null

foreach ($relativePath in $expertFiles) {
    $sourcePath = Join-Path $repositoryRoot $relativePath

    if (-not (Test-Path -LiteralPath $sourcePath -PathType Leaf)) {
        throw "Required MQL5 source file is missing: $relativePath"
    }

    $safeName = [IO.Path]::GetFileNameWithoutExtension($sourcePath)
    $logPath = Join-Path $logDirectory "$safeName.log"

    if (Test-Path -LiteralPath $logPath) {
        Remove-Item -LiteralPath $logPath -Force
    }

    Write-Host "Compiling $relativePath"
    $arguments = @(
        "/compile:$sourcePath",
        "/log:$logPath"
    )
    $process = Start-Process -FilePath $MetaEditorPath -ArgumentList $arguments -PassThru

    if (-not $process.WaitForExit(120000)) {
        Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
        throw "MetaEditor compilation timed out for $relativePath."
    }

    if (-not (Test-Path -LiteralPath $logPath -PathType Leaf)) {
        throw "MetaEditor did not produce a compilation log for $relativePath."
    }

    $logContent = Get-Content -LiteralPath $logPath -Raw
    Write-Host $logContent
    $resultMatch = [regex]::Match($logContent, 'Result:\s*(\d+)\s+errors?,\s*(\d+)\s+warnings?', [Text.RegularExpressions.RegexOptions]::IgnoreCase)

    if (-not $resultMatch.Success) {
        throw "Compilation result could not be verified for $relativePath."
    }

    $errorCount = [int]$resultMatch.Groups[1].Value
    if ($errorCount -ne 0) {
        throw "MetaEditor reported $errorCount compilation error(s) in $relativePath."
    }

    Write-Host "Compilation passed: $relativePath"
}

Write-Host "All configured MQL5 Expert Advisors compiled successfully."
