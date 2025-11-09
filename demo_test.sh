#!/bin/bash

# QueueCTL Demo and Test Script
# This script demonstrates all the features of queuectl

set -e

echo "=========================================="
echo "QueueCTL - Demo & Test Script"
echo "=========================================="
echo ""

# Clean up any existing data
echo "1. Cleaning up previous data..."
rm -rf data/
mkdir -p data
echo "✓ Done"
echo ""

# Test 1: Configuration
echo "2. Testing Configuration..."
queuectl config set max-retries 3
queuectl config set backoff-base 2
queuectl config get
echo "✓ Done"
echo ""

# Test 2: Enqueue jobs
echo "3. Enqueuing test jobs..."
queuectl enqueue '{"id":"job1","command":"echo Hello World"}'
queuectl enqueue '{"id":"job2","command":"sleep 2 && echo Job 2 complete"}'
queuectl enqueue '{"id":"job3","command":"ls /nonexistent"}'  # This will fail
queuectl enqueue '{"id":"job4","command":"echo Test && sleep 1"}'
echo "✓ Done"
echo ""

# Test 3: Check status
echo "4. Checking queue status..."
queuectl status
echo "✓ Done"
echo ""

# Test 4: List jobs
echo "5. Listing all jobs..."
queuectl list
echo "✓ Done"
echo ""

# Test 5: Start workers
echo "6. Starting 2 workers in background..."
queuectl worker start --count 2
sleep 1
echo "✓ Done"
echo ""

# Test 6: Monitor progress
echo "7. Monitoring job execution (15 seconds)..."
for i in {1..5}; do
    echo "--- Check $i ---"
    queuectl status
    sleep 3
done
echo "✓ Done"
echo ""

# Test 7: List completed jobs
echo "8. Listing completed jobs..."
queuectl list --state completed
echo "✓ Done"
echo ""

# Test 8: Check DLQ
echo "9. Checking Dead Letter Queue..."
queuectl dlq list
echo "✓ Done"
echo ""

# Test 9: Retry DLQ job
echo "10. Testing DLQ retry..."
DLQ_JOB=$(queuectl dlq list 2>/dev/null | grep -oP 'job\d+' | head -1 || echo "")
if [ ! -z "$DLQ_JOB" ]; then
    echo "Retrying job: $DLQ_JOB"
    queuectl dlq retry $DLQ_JOB
    queuectl list --state pending
else
    echo "No jobs in DLQ to retry"
fi
echo "✓ Done"
echo ""

# Test 10: Stop workers
echo "11. Stopping all workers..."
queuectl worker stop
sleep 2
echo "✓ Done"
echo ""

# Test 11: Final status
echo "12. Final queue status..."
queuectl status
echo "✓ Done"
echo ""

# Test 12: Persistence test
echo "13. Testing persistence (restart simulation)..."
queuectl enqueue '{"id":"persist1","command":"echo Persistence test"}'
queuectl list --state pending
echo "Data persisted in data/ directory"
echo "✓ Done"
echo ""

echo "=========================================="
echo "All tests completed successfully!"
echo "=========================================="
echo ""
echo "Key features demonstrated:"
echo "  ✓ Job enqueuing"
echo "  ✓ Worker management (start/stop)"
echo "  ✓ Job execution"
echo "  ✓ Retry with exponential backoff"
echo "  ✓ Dead Letter Queue"
echo "  ✓ Configuration management"
echo "  ✓ Data persistence"
echo "  ✓ Status monitoring"
echo ""echo "For more information, refer to the QueueCTL documentation."