<#
.SYNOPSIS
    Launch decomposer + N scorers + executor + observer + dashboard as
    background processes on separate ports. Defaults: 3 scorers
    (N_CONFIRMATIONS in config.py defaults to 2, so 3 gives one to spare),
    max depth 3, 2-6 subgoals per split (LLM's choice within that range).

.EXAMPLE
    $env:ANTHROPIC_API_KEY = "sk-..."
    .\run_all.ps1 -Goal "Plan a two-week trip to Japan"
    .\run_all.ps1 -Goal "..." -ScorerCount 5 -MaxDepth 2 -NumSplits 4

    Ctrl+C stops every agent it started. Dashboard UI: http://127.0.0.1:8765
#>

param(
    [string]$Goal = "Plan a two-week trip to Japan, ensure every subgoal and output is a prompt suitable to feed into another agent in terms of system and user prompt",
    [int]$ScorerCount = 3,
    [int]$MaxDepth = 3,
    [int]$MinSplits = 2,
    [int]$MaxSplits = 6,
    # 0 = unset: let the LLM pick a count within [MinSplits, MaxSplits] for
    # the initial goal, same as any other goal. Set > 0 to pin an exact count.
    [int]$NumSplits = 0,
    [string]$Capabilities = "can_write_text,can_query_api"
)

$ErrorActionPreference = "Stop"

$LogDir = Join-Path $PSScriptRoot "logs"
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir | Out-Null
}

$script:processes = @()

function Start-Agent {
    param(
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][string[]]$AgentArgs
    )
    $outLog = Join-Path $LogDir "$Name.log"
    $errLog = Join-Path $LogDir "$Name.err.log"
    $allArgs = @("-m", "decentralized_decomposer.main") + $AgentArgs
    # Start-Process -ArgumentList does NOT quote array elements that contain
    # spaces (e.g. --submit-goal "Plan a two-week trip to Japan" gets rejoined
    # into a bare, unquoted command line and Windows re-splits it into separate
    # argv entries) -- build an explicitly quoted command-line string instead.
    $argString = ($allArgs | ForEach-Object { '"' + ($_ -replace '"', '\"') + '"' }) -join ' '
    $proc = Start-Process -FilePath python -ArgumentList $argString `
        -RedirectStandardOutput $outLog -RedirectStandardError $errLog `
        -PassThru -NoNewWindow -WorkingDirectory $PSScriptRoot
    $script:processes += $proc
    Write-Host "started $Name (pid $($proc.Id)) -> $outLog"
}

function Stop-AllAgents {
    Write-Host "Stopping agents..."
    foreach ($p in $script:processes) {
        if (-not $p.HasExited) {
            Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue
        }
    }
}

# --max-depth applies to both decomposer and scorer roles (README: the
# decomposer skips a wasted LLM call on over-depth goals, the scorer is the
# one that actually decides atomic-vs-recurse). --num-splits/--min-splits/
# --max-splits are decomposer-only (they shape the initial-goal prompt).
$splitDescription = if ($NumSplits -gt 0) { "exactly $NumSplits (initial goal only)" } else { "$MinSplits-$MaxSplits (LLM's choice)" }

Write-Host "=== Run configuration ==="
Write-Host "  Goal:              $Goal"
Write-Host "  Scorers:           $ScorerCount (config.py's N_CONFIRMATIONS defaults to 2 required to accept a split)"
Write-Host "  Max depth:         $MaxDepth"
Write-Host "  Subgoals per split: $splitDescription"
Write-Host "  Executor capabilities: $Capabilities"
Write-Host "  Dashboard UI:      http://127.0.0.1:8765"
Write-Host "  Logs:              $LogDir\*.log"
Write-Host "=========================="
Write-Host ""

try {
    $decomposerArgs = @(
        "--role", "decomposer", "--port", "4005",
        "--max-depth", "$MaxDepth",
        "--min-splits", "$MinSplits", "--max-splits", "$MaxSplits",
        "--submit-goal", $Goal
    )
    if ($NumSplits -gt 0) {
        $decomposerArgs += @("--num-splits", "$NumSplits")
    }

    Start-Agent -Name "observer"  -AgentArgs @("--role", "observer", "--port", "4001")
    Start-Agent -Name "dashboard" -AgentArgs @("--role", "dashboard", "--port", "4006", "--dashboard-port", "8765")
    for ($i = 1; $i -le $ScorerCount; $i++) {
        $scorerPort = 4010 + $i
        Start-Agent -Name "scorer$i" -AgentArgs @("--role", "scorer", "--port", "$scorerPort", "--max-depth", "$MaxDepth")
    }
    Start-Agent -Name "executor"   -AgentArgs @("--role", "executor", "--port", "4004", "--capabilities", $Capabilities)
    Start-Agent -Name "decomposer" -AgentArgs $decomposerArgs

    Write-Host ""
    Write-Host "All agents running. Dashboard UI: http://127.0.0.1:8765"
    Write-Host "Logs: $LogDir\*.log"
    Write-Host "Press Ctrl+C to stop."
    Write-Host ""

    Wait-Process -Id ($script:processes | ForEach-Object { $_.Id })
}
finally {
    Stop-AllAgents
}
