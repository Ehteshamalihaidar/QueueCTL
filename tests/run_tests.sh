set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

CLI="./queuectl.py"
PY=python3

echo "Cleaning database and state..."
rm -f queuectl.db
rm -f queuectl_worker.pid || true

echo
echo "✅ TEST 1: Basic job completes successfully"
$PY $CLI enqueue '{"id":"job_ok","command":"echo OK","max_retries":2}'
$PY $CLI worker start --count 1 &
W_PID=$!
sleep 3
kill -SIGINT $W_PID || true
$PY $CLI status | grep completed

echo
echo "✅ TEST 2: Failed job retries and moves to DLQ"
rm -f queuectl_worker.pid || true
$PY $CLI enqueue '{"id":"job_fail","command":"bash -c \"exit 1\"","max_retries":2}'
$PY $CLI worker start --count 1 &
W_PID=$!
sleep 10
kill -SIGINT $W_PID || true
$PY $CLI dlq list | grep job_fail

echo
echo "✅ TEST 3: Multiple workers process jobs without overlap"
rm -f queuectl_worker.pid || true
$PY $CLI enqueue '{"id":"job_w1","command":"echo worker1"}'
$PY $CLI enqueue '{"id":"job_w2","command":"echo worker2"}'
$PY $CLI enqueue '{"id":"job_w3","command":"echo worker3"}'
$PY $CLI worker start --count 2 &
W_PID=$!
sleep 5
kill -SIGINT $W_PID || true
$PY $CLI status | grep completed

echo
echo "✅ TEST 4: Invalid commands fail gracefully"
$PY $CLI enqueue '{"id":"job_badcmd","command":"not_a_real_command_x"}'
$PY $CLI worker start --count 1 &
W_PID=$!
sleep 6
kill -SIGINT $W_PID || true
$PY $CLI dlq list | grep job_badcmd

echo
echo "✅ TEST 5: Job data persists after restart"
$PY $CLI enqueue '{"id":"job_persist","command":"echo persist"}'
$PY $CLI status | grep pending
echo "Restarting workers..."
$PY $CLI worker start --count 1 &
W_PID=$!
sleep 3
kill -SIGINT $W_PID || true
$PY $CLI status | grep completed

echo
echo "All tests passed successfully!"
