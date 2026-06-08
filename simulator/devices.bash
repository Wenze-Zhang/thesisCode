#!/usr/bin/env bash

set -euo pipefail

TB_HOST="${TB_HOST:-http://thingsboard:8080}"

GREEN='\033[0;32m'; RED='\033[0;31m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# test curl & python command
for cmd in curl python; do
  command -v "$cmd" >/dev/null || error "Missing command: $cmd"
done

# wait for ThingsBoard 
info "Waiting for ThingsBoard at $TB_HOST ..."
for i in $(seq 1 150); do
  if curl -sf "$TB_HOST/login" -o /dev/null 2>/dev/null; then
    info "ThingsBoard is ready"
    break
  fi
  if [ "$i" -eq 150 ]; then
    error "ThingsBoard is not ready yet"
  fi
  [ $((i % 15)) -eq 0 ] && info "  still waiting... (${i}x2s elapsed)"
  sleep 2
done

# create log directory
mkdir -p logs
PIDS=()
launch() {
  local device_type="$1" device="$2" interval="$3"
  python -u simulator.py "$device_type" "$device" "$interval" \
    > "logs/${device_type}.log" 2>&1 &
  PIDS+=($!)
  info "  started $device_type -> $device (pid $!) -> logs/${device_type}.log"
}

info "Launching simulators..."
launch energy  "energy-meter-001"   5
launch climate "sim-climate-sensor" 5
launch water   "sim-water-meter"    10
launch air     "sim-air-quality-01" 15
launch ev      "sim-ev-charger-01"  10

# stop
cleanup() {
  echo
  info "Stopping all simulators..."
  for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
  exit 0
}
trap cleanup INT TERM

info "All simulators running. Tail combined logs:  tail -f logs/*.log"
wait
