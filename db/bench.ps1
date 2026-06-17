<#
.SYNOPSIS
  SDI engine benchmark: seed a synthetic dataset at a given scale, run SP_EOD_RUN,
  measure per-job execution time (from T_EOD_RUN), then APPEND one row to
  db/perf-history.csv. Run after each refactor to watch for perf regressions.

.EXAMPLE
  ./bench.ps1                      # scale large (default), instance .\SQLEXPRESS
  ./bench.ps1 -Scale small         # quick harness check
  ./bench.ps1 -Scale large -KeepDb # keep DB after run for inspection
#>
[CmdletBinding()]
param(
    [ValidateSet('small','medium','large')] [string]$Scale = 'large',
    [string]$Server = '.\SQLEXPRESS',
    [string]$Database = 'SDI_BENCH',
    [string]$BusinessDate = '2026-01-02',
    [string]$HistoryFile,
    [switch]$KeepDb
)
$ErrorActionPreference = 'Stop'
$dbDir   = $PSScriptRoot
$repoDir = Split-Path $dbDir -Parent
if (-not $HistoryFile) { $HistoryFile = Join-Path $dbDir 'perf-history.csv' }

# scale -> (nCust, nSi, nTick)
$map = @{
    small  = @(1000,  3, 20)
    medium = @(10000, 5, 25)
    large  = @(50000, 5, 25)
}
$nCust, $nSi, $nTick = $map[$Scale]
$nPos = $nCust * $nSi * $nTick

function Invoke-Sql {
    param([string]$Query, [string]$InputFile, [string[]]$Vars, [switch]$Raw, [string]$Db = $Database)
    $a = @('-S', $Server, '-E', '-b', '-f', '65001')
    if ($Db)        { $a += @('-d', $Db) }
    if ($Vars)      { foreach ($v in $Vars) { $a += @('-v', $v) } }
    if ($Raw)       { $a += @('-h','-1','-W','-s',',') }
    if ($InputFile) { $a += @('-i', $InputFile) } elseif ($Query) { $a += @('-Q', $Query) }
    $out = & sqlcmd @a
    if ($LASTEXITCODE -ne 0) { throw "sqlcmd failed (exit $LASTEXITCODE): $out" }
    return $out
}

Write-Host "== BENCH scale=$Scale | nCust=$nCust nSi=$nSi nTick=$nTick -> $nPos positions ==" -ForegroundColor Cyan

# 1. fresh DB
Invoke-Sql -Db '' -Query "IF DB_ID('$Database') IS NOT NULL BEGIN ALTER DATABASE [$Database] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [$Database]; END; CREATE DATABASE [$Database];" | Out-Null

# 2. schema + engine
Write-Host "  -> 01_TABLES + 02_SP_ENGINE..."
Invoke-Sql -InputFile (Join-Path $dbDir '01_TABLES.sql')    | Out-Null
Invoke-Sql -InputFile (Join-Path $dbDir '02_SP_ENGINE.sql') | Out-Null

# 3. seed + run (measured)
Write-Host "  -> 04_BENCH seed + EOD (large scale may take a while)..."
$vars = @("NCUST=$nCust","NSI=$nSi","NTICK=$nTick","DT=$BusinessDate")
Invoke-Sql -InputFile (Join-Path $dbDir '04_BENCH.sql') -Vars $vars | Out-Null

# 4. read per-job durations as one csv row
$pivot = @"
SET NOCOUNT ON;
SELECT
  SUM(DATEDIFF(MILLISECOND,C_STARTED_AT,C_ENDED_AT)),
  MAX(CASE WHEN C_JOB='J01_SYNC_FO'   THEN DATEDIFF(MILLISECOND,C_STARTED_AT,C_ENDED_AT) END),
  MAX(CASE WHEN C_JOB='J07_COMPUTE'   THEN DATEDIFF(MILLISECOND,C_STARTED_AT,C_ENDED_AT) END),
  MAX(CASE WHEN C_JOB='J11_SI_AGG'    THEN DATEDIFF(MILLISECOND,C_STARTED_AT,C_ENDED_AT) END),
  MAX(CASE WHEN C_JOB='J12_SI_INDEX'  THEN DATEDIFF(MILLISECOND,C_STARTED_AT,C_ENDED_AT) END),
  MAX(CASE WHEN C_JOB='J13_RECONCILE' THEN DATEDIFF(MILLISECOND,C_STARTED_AT,C_ENDED_AT) END),
  MAX(CASE WHEN C_JOB='J14_SNAPSHOT'  THEN DATEDIFF(MILLISECOND,C_STARTED_AT,C_ENDED_AT) END),
  SUM(CASE WHEN C_STATUS<>'DONE' THEN 1 ELSE 0 END)
FROM T_EOD_RUN WHERE C_BUSINESS_DATE='$BusinessDate';
"@
$rowOut = Invoke-Sql -Query $pivot -Raw | Where-Object { $_ -match '\d' } | Select-Object -First 1
$f = $rowOut -split ','
$total,$j01,$j07,$j11,$j12,$j13,$j14,$nbad = $f
if ([int]$nbad -ne 0) { throw "Found $nbad job(s) not DONE - bench invalid, not writing history." }

# 5. git context
Push-Location $repoDir
try {
    $commit  = (& git rev-parse --short HEAD).Trim()
    $subject = (& git log -1 --pretty=%s).Trim() -replace '"','""' -replace '[\r\n]',' '
} finally { Pop-Location }
$ts = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')

# 6. append CSV (write header if missing)
$header = 'timestamp_utc,commit,subject,scale,n_customers,n_positions,total_ms,J01_ms,J07_ms,J11_ms,J12_ms,J13_ms,J14_ms'
if (-not (Test-Path $HistoryFile)) { Set-Content -Path $HistoryFile -Value $header -Encoding utf8 }
$line = '{0},{1},"{2}",{3},{4},{5},{6},{7},{8},{9},{10},{11},{12}' -f `
        $ts,$commit,$subject,$Scale,$nCust,$nPos,$total,$j01,$j07,$j11,$j12,$j13,$j14
Add-Content -Path $HistoryFile -Value $line -Encoding utf8

# 7. summary
Write-Host "`n== RESULT (ms) ==" -ForegroundColor Green
Write-Host ("  J01_SYNC_FO={0}  J07_COMPUTE(MTM)={1}  J11_SI_AGG={2}" -f $j01,$j07,$j11)
Write-Host ("  J12_SI_INDEX={0}  J13_RECONCILE={1}  J14_SNAPSHOT={2}" -f $j12,$j13,$j14)
Write-Host ("  TOTAL={0} ms  |  commit {1}" -f $total,$commit) -ForegroundColor Yellow
Write-Host ("  -> appended: {0}" -f $HistoryFile)

if (-not $KeepDb) {
    Invoke-Sql -Db '' -Query "ALTER DATABASE [$Database] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [$Database];" | Out-Null
}
