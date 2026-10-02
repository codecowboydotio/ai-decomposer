#!/usr/bin/env bash
# Launch decomposer + two scorers + executor + observer + dashboard as
# background processes on separate ports (N_CONFIRMATIONS defaults to 2, so
# two scorers is enough for a split to actually get accepted).
#
# Usage:
#   export ANTHROPIC_API_KEY=sk-...
#   ./run_all.sh "Plan a two-week trip to Japan"
#   ./run_all.sh "Plan a two-week trip to Japan" 2   # 2s delay between each agent start
#   ./run_all.sh "Plan a two-week trip to Japan" 0 127.0.0.1   # dashboard-only, no LAN access
#
# Ctrl-C stops every agent it started. Dashboard UI: http://127.0.0.1:8765

set -euo pipefail

GOAL="${1:-Plan a two-week trip to Japan}"
# Delay (seconds, may be fractional) between starting each agent process, to
# stagger port binding / gossip-mesh formation instead of launching everything
# at once.
START_DELAY="${2:-0}"
# Interface the dashboard's HTTP server binds to. 0.0.0.0 makes it reachable
# from other machines on the network, not just this one.
DASHBOARD_HOST="${3:-0.0.0.0}"
LOG_DIR="logs"
mkdir -p "$LOG_DIR"

PIDS=()
cleanup() {
    echo "Stopping agents..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
}
# EXIT alone covers normal completion; INT/TERM must also exit explicitly so
# a Ctrl-C is handled once and the script doesn't fall back into `wait` with
# nothing left to wait on.
trap cleanup EXIT
trap 'cleanup; exit 130' INT TERM

echo "Start delay: ${START_DELAY}s between agents"

start() {
    local name="$1"
    shift
    python -m decentralized_decomposer.main "$@" > "$LOG_DIR/$name.log" 2>&1 &
    PIDS+=("$!")
    echo "started $name (pid $!) -> $LOG_DIR/$name.log"
    sleep "$START_DELAY"
}

start observer   --role observer   --port 4001
start dashboard   --role dashboard  --port 4006 --dashboard-port 8765 --dashboard-host "$DASHBOARD_HOST"
start scorer1     --role scorer     --port 4002
start scorer2     --role scorer     --port 4003
start executor    --role executor   --port 4004 --capabilities can_write_text,can_query_api
start decomposer  --role decomposer --port 4005 --submit-goal "$GOAL"

echo "All agents running. Dashboard UI: http://127.0.0.1:8765 (bound to ${DASHBOARD_HOST}:8765)"
echo "Tail logs with: tail -f $LOG_DIR/*.log"
echo "Press Ctrl-C to stop."
wait
