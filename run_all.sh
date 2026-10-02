#!/usr/bin/env bash
# Launch decomposer + two scorers + executor + observer as background
# processes on separate ports (N_CONFIRMATIONS defaults to 2, so two
# scorers is enough for a split to actually get accepted).
#
# Usage:
#   export ANTHROPIC_API_KEY=sk-...
#   ./run_all.sh "Plan a two-week trip to Japan"
#   ./run_all.sh "Plan a two-week trip to Japan" 2   # 2s delay between each agent start
#
# Ctrl-C stops every agent it started.

set -euo pipefail

GOAL="${1:-Plan a two-week trip to Japan}"
# Delay (seconds, may be fractional) between starting each agent process, to
# stagger port binding / gossip-mesh formation instead of launching everything
# at once.
START_DELAY="${2:-0}"
LOG_DIR="logs"
mkdir -p "$LOG_DIR"

PIDS=()
cleanup() {
    echo "Stopping agents..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
}
trap cleanup EXIT INT TERM

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
start scorer1     --role scorer     --port 4002
start scorer2     --role scorer     --port 4003
start executor    --role executor   --port 4004 --capabilities can_write_text,can_query_api
start decomposer  --role decomposer --port 4005 --submit-goal "$GOAL"

echo "All agents running. Tail logs with: tail -f $LOG_DIR/*.log"
echo "Press Ctrl-C to stop."
wait
