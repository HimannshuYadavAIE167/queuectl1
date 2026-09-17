# QueueCTL

A production-grade CLI-based background job queue system built in Python with persistent storage, parallel worker processes, automatic retry with exponential backoff, and Dead Letter Queue (DLQ) management.

**📹 [Link to CLI Demo Video](https://drive.google.com/file/d/18ecKhhzC-ocEM6eGS1wXEXaTIwnJNqUd/view?usp=sharing)**

---

## 🎯 Overview

`queuectl` is a robust job queue system designed to manage background jobs efficiently. It handles job execution through parallel worker processes, implements intelligent retry mechanisms with exponential backoff, and maintains a Dead Letter Queue for permanently failed jobs. All operations are accessible through an intuitive command-line interface.

### Key Features

- **Persistent Job Storage**: Jobs are stored on disk as JSON files and survive system restarts
- **Parallel Processing**: Run multiple worker processes concurrently using Python's `multiprocessing`
- **Atomic Operations**: File-based locking mechanism prevents race conditions and duplicate job processing
- **Intelligent Retry Logic**: Failed jobs automatically retry with exponential backoff (`delay = base ^ attempts`)
- **Dead Letter Queue (DLQ)**: Jobs exhausting retry attempts are moved to DLQ for manual inspection
- **Graceful Shutdown**: Workers handle `SIGINT`/`SIGTERM` signals to finish current jobs before exiting
- **Configurable System**: Manage `max_retries`, `backoff_base`, and other settings via CLI
- **Job Timeout Handling**: Automatic timeout for long-running jobs (300 seconds default)

---

## 📋 Table of Contents

- [Setup Instructions](#-setup-instructions)
- [Usage Examples](#-usage-examples)
- [Architecture Overview](#-architecture-overview)
- [Job Specification](#-job-specification)
- [Assumptions & Trade-offs](#-assumptions--trade-offs)
- [Testing Instructions](#-testing-instructions)
- [Evaluation Checklist](#-evaluation-checklist)

---

## 🚀 Setup Instructions

### Prerequisites

- **Python 3.7+** installed on your system
- **Git** for cloning the repository
- **(Windows Users)** Git Bash or equivalent shell that provides `bash.exe` and Unix commands (`ls`, `sleep`, etc.)

### Installation Steps

1. **Clone the Repository**

   ```bash
   git clone https://github.com/HimannshuYadavAIE167/queuectl.git
   cd queuectl
   ```

2. **Create a Virtual Environment**

   ```bash
   # On macOS/Linux
   python3 -m venv venv
   source venv/bin/activate

   # On Windows (Git Bash)
   python -m venv venv
   source venv/Scripts/activate
   ```

3. **Install the Package**

   Install `queuectl` in editable mode with all dependencies:

   ```bash
   pip install -e .
   ```

   This installs the `queuectl` CLI command and required packages like `tabulate`.

4. **Verify Installation**

   ```bash
   queuectl --help
   ```

   You should see the help menu with all available commands.

---

## 💻 Usage Examples

### Configuration Management

Configure system behavior including retry limits and backoff timing:

```bash
# Set maximum retry attempts for jobs
queuectl config set max-retries 3

# Set exponential backoff base (e.g., 2^1, 2^2, 2^3 seconds)
queuectl config set backoff-base 2

# View current configuration
queuectl config get
```

**Example Output:**
```
Current Configuration:
  max_retries = 3
  backoff_base = 2
  data_dir = data
```

### Enqueue Jobs

Add new jobs to the queue with a unique ID and command:

```bash
# Enqueue a simple successful job
queuectl enqueue '{"id":"job1","command":"echo Hello World"}'

# Enqueue a job that will fail (for testing retry logic)
queuectl enqueue '{"id":"job3","command":"ls /nonexistent"}'

# Enqueue a longer-running job
queuectl enqueue '{"id":"job2","command":"sleep 2 && echo Job 2 complete"}'

# Enqueue multiple jobs at once
queuectl enqueue '{"id":"job4","command":"echo Test && sleep 1"}'
```

**Example Output:**
```
✓ Job 'job1' enqueued successfully
  Command: echo Hello World
  Max retries: 3
```

### Worker Management

#### Start Workers

Start worker processes to process the queue (runs in foreground):

```bash
# Start 2 worker processes
queuectl worker start --count 2
```

**Example Output:**
```
Starting 2 workers in foreground (Press Ctrl+C to stop)...
Started worker process (PID: 14056)
Started worker process (PID: 20376)
[Worker 4cac2921] Started (PID: 14056)
[Worker ece552a7] Started (PID: 20376)
[Worker 4cac2921] Processing job job1: echo Hello World
[Worker 4cac2921] Job job1 completed successfully
[Worker ece552a7] Processing job job2: sleep 2 && echo Job 2 complete
[Worker ece552a7] Job job2 completed successfully
```

**Note:** Press `Ctrl+C` to stop workers gracefully. Workers will finish their current job before exiting.

#### Stop Workers

Stop all active worker processes:

```bash
queuectl worker stop
```

**Example Output:**
```
Stopping worker (PID: 14056)...
Stopping worker (PID: 20376)...
All workers stopped
```

### Queue Status

Get a comprehensive summary of the queue state:

```bash
queuectl status
```

**Example Output:**
```
==================================================
QUEUE STATUS
==================================================

Job States:
  Pending:    0
  Processing: 0
  Completed:  3
  Failed:     0
  Dead (DLQ): 1

Active Workers: 2
==================================================
```

### List Jobs

View all jobs or filter by specific states:

```bash
# List all jobs in the queue
queuectl list

# List only pending jobs
queuectl list --state pending

# List only failed jobs
queuectl list --state failed

# List completed jobs
queuectl list --state completed
```

**Example Output:**
```
All jobs:
+------+--------------------------------+-----------+----------+-------------+---------------------+
| ID   | Command                        | State     | Attempts | Max Retries | Created At          |
+======+================================+===========+==========+=============+=====================+
| job1 | echo Hello World               | completed |        1 |           3 | 2025-11-09T17:25:55 |
+------+--------------------------------+-----------+----------+-------------+---------------------+
| job2 | sleep 2 && echo Job 2 complete | completed |        1 |           3 | 2025-11-09T17:25:55 |
+------+--------------------------------+-----------+----------+-------------+---------------------+
| job4 | echo Test && sleep 1           | completed |        1 |           3 | 2025-11-09T17:25:55 |
+------+--------------------------------+-----------+----------+-------------+---------------------+
```

### Dead Letter Queue (DLQ) Management

Handle permanently failed jobs:

```bash
# List all jobs in the Dead Letter Queue
queuectl dlq list

# Retry a specific job from the DLQ (resets attempts and moves to pending)
queuectl dlq retry job3
```

**Example Output:**
```
Dead Letter Queue:
+------+-----------------+-------+----------+-------------+---------------------+
| ID   | Command         | State | Attempts | Max Retries | Created At          |
+======+=================+=======+==========+=============+=====================+
| job3 | ls /nonexistent | dead  |        4 |           3 | 2025-11-09T17:25:55 |
+------+-----------------+-------+----------+-------------+---------------------+

Total: 1 job(s)
```

---

## 🏗️ Architecture Overview

### Job Lifecycle

Jobs transition through five states during their lifecycle:

| State | Description |
|-------|-------------|
| `pending` | Job is waiting in the queue to be picked up by a worker |
| `processing` | A worker has locked the job and is currently executing it |
| `completed` | Job executed successfully (exit code 0) |
| `failed` | Job failed (non-zero exit code or timeout), eligible for retry |
| `dead` | Job exhausted all retry attempts and moved to DLQ |

**Lifecycle Flow:**
```
pending → processing → completed ✓
              ↓ (on failure)
            failed → pending (retry with backoff)
              ↓ (max_retries exhausted)
            dead (DLQ)
```

### Data Persistence

The system uses a file-based persistence model for simplicity and portability:

```
data/
├── jobs.json       # Main job store (pending, processing, completed, failed)
├── dlq.json        # Dead Letter Queue (dead jobs)
├── config.json     # System configuration
├── locks.json      # Job lock tracking for concurrency control
└── *.lock          # Temporary lock files for atomic operations
```

**File Descriptions:**

- **`jobs.json`**: Contains all active jobs in various states (pending through failed)
- **`dlq.json`**: Stores permanently failed jobs that exhausted retry attempts
- **`config.json`**: System settings like `max_retries`, `backoff_base`, and `data_dir`
- **`locks.json`**: Tracks which worker has locked which job to prevent race conditions
- **`*.lock` files**: Filesystem locks ensuring atomic read-modify-write operations

### Worker Logic & Concurrency Control

#### Process Management

- Workers are spawned using Python's `multiprocessing.Process`
- Each worker runs independently in its own process space
- Parent process tracks worker PIDs for management and graceful shutdown

#### Job Selection Algorithm

To prevent race conditions where multiple workers grab the same job:

1. **Acquire Lock**: Worker attempts to acquire an exclusive file lock on `jobs.json.lock`
2. **Read Jobs**: Worker reads `jobs.json` and finds the first job in `pending` state
3. **Claim Job**: Worker writes the job ID to `locks.json` to "claim" it
4. **Update State**: Job state changes from `pending` to `processing`
5. **Release Lock**: Filesystem lock is released, allowing other workers to proceed

This file-based spin lock ensures only one worker can modify job states at a time.

#### Job Execution

```python
# Worker executes job command
result = subprocess.run(
    command,
    shell=True,
    capture_output=True,
    timeout=300  # 5-minute timeout
)

# Determine outcome based on exit code
if result.returncode == 0:
    mark_completed()
else:
    schedule_retry_or_move_to_dlq()
```

#### Retry Logic with Exponential Backoff

When a job fails:

1. Increment `attempts` counter
2. Calculate backoff delay: `delay = backoff_base ^ attempts`
3. Set `next_run_at = current_time + delay`
4. If `attempts < max_retries`: job stays in `failed` state (will retry)
5. If `attempts >= max_retries`: job moves to `dead` state (DLQ)

**Example Backoff Timeline** (base=2, max_retries=3):
- Attempt 1 fails → wait 2^1 = 2 seconds
- Attempt 2 fails → wait 2^2 = 4 seconds
- Attempt 3 fails → wait 2^3 = 8 seconds
- Attempt 4 fails → move to DLQ

#### Graceful Shutdown

Workers register signal handlers for `SIGINT` (Ctrl+C) and `SIGTERM`:

```python
signal.signal(signal.SIGINT, self._handle_shutdown)
signal.signal(signal.SIGTERM, self._handle_shutdown)
```

On receiving shutdown signal:
1. Set `self.running = False` to stop main loop
2. Finish currently executing job
3. Release all locks
4. Exit cleanly

---

## 📦 Job Specification

Each job contains the following fields:

```json
{
  "id": "unique-job-id",
  "command": "echo 'Hello World'",
  "state": "pending",
  "attempts": 0,
  "max_retries": 3,
  "created_at": "2025-11-04T10:30:00Z",
  "updated_at": "2025-11-04T10:30:00Z",
  "next_run_at": null,
  "worker_id": null
}
```

**Field Descriptions:**

- `id` (string, required): Unique identifier for the job
- `command` (string, required): Shell command to execute
- `state` (string): Current job state (pending/processing/completed/failed/dead)
- `attempts` (integer): Number of execution attempts
- `max_retries` (integer): Maximum retry attempts before moving to DLQ
- `created_at` (ISO timestamp): Job creation time
- `updated_at` (ISO timestamp): Last modification time
- `next_run_at` (ISO timestamp, nullable): When to retry failed job (for backoff)
- `worker_id` (string, nullable): ID of worker currently processing the job

---

## 🤔 Assumptions & Trade-offs

### Storage: JSON vs Database

**Decision**: JSON file-based storage

**Rationale**:
- ✅ Simple implementation with no external dependencies
- ✅ Human-readable for debugging
- ✅ Portable across systems
- ✅ Sufficient for assignment scope

**Trade-offs**:
- ❌ **Not scalable**: Each job update requires reading/writing entire file
- ❌ **Performance bottleneck**: ~O(n) complexity for job operations
- ❌ **File system I/O**: High disk usage with frequent updates

**Production Alternative**: For real-world systems with 1000+ jobs, use SQLite (embedded) or PostgreSQL (distributed) with:
- Row-level locking for true concurrent access
- Indexed queries for fast lookups
- ACID transactions for data integrity

### Concurrency: File Lock vs Database Lock

**Decision**: Custom file-based spin lock using `fcntl` (Unix) / `msvcrt` (Windows)

**Rationale**:
- ✅ Works with JSON file storage
- ✅ No additional infrastructure needed
- ✅ Prevents race conditions effectively

**Trade-offs**:
- ❌ **Pessimistic locking**: Only one worker can access jobs at a time
- ❌ **Potential bottleneck**: High worker count = more lock contention
- ❌ **Platform-specific code**: Different implementations for Unix/Windows

**Production Alternative**: Database with optimistic locking or row-level locks for better concurrency.

### Platform Compatibility: Bash Dependency

**Decision**: Require `bash` executable for command execution on Windows

**Rationale**:
- ✅ Consistent command execution across platforms
- ✅ Supports demo script with Unix commands (`ls`, `sleep`)
- ✅ Git Bash is widely available on Windows

**Trade-offs**:
- ❌ **Additional dependency**: Windows users must install Git Bash
- ❌ **Not native**: Adds layer of indirection on Windows

**Note**: For production, consider platform-specific command adapters or use Python-native alternatives.

### Worker Management: Foreground vs Background

**Decision**: Workers run in foreground by default

**Rationale**:
- ✅ Simpler process lifecycle management
- ✅ Easy cleanup on Ctrl+C
- ✅ Clear visibility of worker output
- ✅ Avoids zombie processes

**Trade-offs**:
- ❌ **Terminal blocking**: User must keep terminal open
- ❌ **No daemonization**: Workers don't survive terminal closure

**Production Alternative**: Implement proper daemonization with PID file management, or use process supervisors like `systemd`, `supervisord`, or containerization.

### Job Timeout: Fixed vs Configurable

**Decision**: Fixed 300-second (5-minute) timeout

**Rationale**:
- ✅ Prevents hung processes from blocking workers
- ✅ Reasonable default for most background jobs
- ✅ Simpler implementation

**Trade-offs**:
- ❌ **Not flexible**: Some jobs may need longer/shorter timeouts
- ❌ **Hardcoded**: Requires code change to adjust

**Future Enhancement**: Make timeout configurable per-job or globally via config.

---

## 🧪 Testing Instructions

### End-to-End Demo Script

The `demo_test.sh` script validates all core features in a real-world scenario.

**Steps to Run:**

1. **Activate Virtual Environment**
   ```bash
   source venv/bin/activate  # Linux/Mac
   source venv/Scripts/activate  # Windows (Git Bash)
   ```

2. **Execute Demo Script**
   ```bash
   ./demo_test.sh
   ```

3. **Important**: When workers start (Step 6), the terminal will block. **Wait 10-15 seconds** for jobs to process, then press **`Ctrl+C`** to gracefully stop workers and continue the script.

**What the Script Tests:**

1. ✅ Clean data directory initialization
2. ✅ Configuration management (`set max-retries`, `set backoff-base`)
3. ✅ Job enqueuing (4 jobs: 3 successful, 1 failing)
4. ✅ Queue status reporting
5. ✅ Multi-worker job processing
6. ✅ Successful job completion
7. ✅ Failed job retry with backoff
8. ✅ DLQ population after retry exhaustion
9. ✅ Job retry from DLQ
10. ✅ Worker graceful shutdown
11. ✅ Data persistence across operations

### Unit Tests

Run individual test suites to verify specific components:

```bash
# Test job queue logic (enqueue, dequeue, state transitions)
python -m unittest tests.test_queue

# Test worker logic (execution, retry, backoff)
python -m unittest tests.test_worker

# Test storage layer (file operations, locking)
python -m unittest tests.test_storage

# Run all tests
python -m unittest discover tests
```

### Manual Testing Scenarios

#### Test 1: Basic Job Success
```bash
queuectl enqueue '{"id":"test1","command":"echo Success"}'
queuectl worker start --count 1
# Wait for completion, then Ctrl+C
queuectl list --state completed
```

#### Test 2: Retry with Backoff
```bash
queuectl config set max-retries 3
queuectl config set backoff-base 2
queuectl enqueue '{"id":"test2","command":"exit 1"}'
queuectl worker start --count 1
# Observe retry delays: 2s, 4s, 8s
# Job moves to DLQ after 3 attempts
queuectl dlq list
```

#### Test 3: Multiple Workers (No Overlap)
```bash
# Enqueue multiple jobs
queuectl enqueue '{"id":"job1","command":"sleep 3 && echo Job 1"}'
queuectl enqueue '{"id":"job2","command":"sleep 3 && echo Job 2"}'
queuectl enqueue '{"id":"job3","command":"sleep 3 && echo Job 3"}'

# Start 3 workers - each should pick different job
queuectl worker start --count 3
# Verify no duplicate processing in logs
```

#### Test 4: Persistence Across Restarts
```bash
queuectl enqueue '{"id":"persist1","command":"echo Test"}'
queuectl status  # Shows 1 pending
# Restart system / close terminal
queuectl status  # Still shows 1 pending
queuectl worker start --count 1
queuectl list --state completed  # Job completed
```

#### Test 5: Invalid Command Handling
```bash
queuectl enqueue '{"id":"invalid","command":"nonexistent_command_xyz"}'
queuectl worker start --count 1
# Job fails and retries
queuectl list --state failed
# Eventually moves to DLQ
```

---

## ✅ Evaluation Checklist

### Core Functionality (40%)
- [x] Enqueue jobs with unique IDs and commands
- [x] Multiple parallel worker processes
- [x] Job execution with exit code handling
- [x] Automatic retry with exponential backoff
- [x] Dead Letter Queue for failed jobs
- [x] Graceful worker shutdown (SIGINT/SIGTERM)

### Code Quality (20%)
- [x] Clear separation of concerns (CLI, Queue, Worker, Storage modules)
- [x] Consistent naming conventions
- [x] Comprehensive error handling
- [x] Type hints and documentation
- [x] Modular, maintainable code structure

### Robustness (20%)
- [x] Atomic operations with file locking
- [x] Race condition prevention
- [x] Invalid command handling
- [x] Job timeout handling (300s)
- [x] Duplicate job ID validation
- [x] Edge case handling (empty queue, missing config, etc.)

### Documentation (10%)
- [x] Comprehensive README with setup instructions
- [x] Usage examples with sample outputs
- [x] Architecture overview with diagrams
- [x] Clear explanation of assumptions and trade-offs
- [x] Testing instructions

### Testing (10%)
- [x] End-to-end demo script (`demo_test.sh`)
- [x] Unit tests for core modules
- [x] Manual testing scenarios documented
- [x] Core flows validated

### Bonus Features
- [x] Job timeout handling (5-minute default)
- [ ] Job priority queues
- [ ] Scheduled/delayed jobs
- [ ] Job output logging
- [ ] Execution metrics/stats
- [ ] Web dashboard for monitoring

---

## 🔧 Troubleshooting

### Issue: Workers not processing jobs

**Solution**: Check if jobs are in `pending` state and workers are running:
```bash
queuectl status
queuectl list --state pending
```

### Issue: "bash not found" error on Windows

**Solution**: Install Git Bash and ensure it's in your PATH:
```bash
where bash  # Should show bash.exe location
```

### Issue: Jobs stuck in `processing` state

**Solution**: Stop all workers and manually reset job states:
```bash
queuectl worker stop
# Manually edit data/jobs.json to change state from "processing" to "pending"
```

### Issue: Permission denied on lock files

**Solution**: Clear stale lock files:
```bash
rm data/*.lock
```

---

## 📚 Additional Resources

- **Python Multiprocessing**: [Official Docs](https://docs.python.org/3/library/multiprocessing.html)
- **File Locking**: `fcntl` (Unix), `msvcrt` (Windows)
- **Subprocess Management**: [Official Docs](https://docs.python.org/3/library/subprocess.html)
- **CLI with Click/Argparse**: Used for command-line interface

---

## 👤 Author

**Your Name**
- GitHub: [@HimannshuYadavAIE167](https://github.com/HimannshuYadavAIE167)
- Email: himanshu80023@gmail.com

---

## 🙏 Acknowledgments

Built as a submission for QueueCTL Backend Developer Internship Assignment. Special thanks to the team for providing detailed requirements and evaluation criteria.
