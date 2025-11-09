import argparse
import json
import sys
import multiprocessing
from tabulate import tabulate
from .queue import JobQueue
from .worker import Worker, WorkerManager
from .config import Config

def format_job_table(jobs):
    """Format jobs as a table."""
    if not jobs:
        return "No jobs found"
    
    headers = ["ID", "Command", "State", "Attempts", "Max Retries", "Created At"]
    rows = []
    for job in jobs:
        cmd = job.command[:40] + "..." if len(job.command) > 40 else job.command
        rows.append([
            job.id[:12],
            cmd,
            job.state,
            job.attempts,
            job.max_retries,
            job.created_at[:19]
        ])
    return tabulate(rows, headers=headers, tablefmt="grid")

def cmd_enqueue(args):
    """Enqueue a new job."""
    try:
        job_data = json.loads(args.job_json)
        
        # Validate required fields
        if "id" not in job_data or "command" not in job_data:
            print("Error: Job must have 'id' and 'command' fields")
            return 1
        
        queue = JobQueue()
        job = queue.enqueue(job_data)
        print(f"✓ Job '{job.id}' enqueued successfully")
        print(f"  Command: {job.command}")
        print(f"  Max retries: {job.max_retries}")
        return 0
        
    except json.JSONDecodeError:
        print("Error: Invalid JSON format")
        return 1
    except Exception as e:
        print(f"Error: {str(e)}")
        return 1

def cmd_worker_start(args):
    """Start worker processes."""
    try:  # <-- MODIFIED: Wrap in try/except
        manager = WorkerManager()
        
        if args.count == 1:
            # Single worker in foreground
            print("Starting 1 worker in foreground (Press Ctrl+C to stop)...")
            worker = Worker()
            worker.start()
        else:
            # Multiple workers in foreground
            # MODIFIED: Update print message
            print(f"Starting {args.count} workers in foreground (Press Ctrl+C to stop)...")
            processes = manager.start_workers(args.count) # <-- MODIFIED: Get process list
            
            # --- NEW BLOCK ---
            # Wait for all processes to finish (join).
            # This keeps the main 'queuectl' command alive.
            for p in processes:
                p.join()
            # --- END NEW BLOCK ---
        
        return 0

    except KeyboardInterrupt:  # <-- NEW: Catch Ctrl+C
        print("\nCaught interrupt, stopping all workers...")
        manager = WorkerManager() # Re-init manager
        manager.stop_workers()
        return 1
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return 1

def cmd_worker_stop(args):
    """Stop all worker processes."""
    try:
        manager = WorkerManager()
        manager.stop_workers()
        return 0
    except Exception as e:
        print(f"Error: {str(e)}")
        return 1

def cmd_status(args):
    """Show queue status."""
    try:
        queue = JobQueue()
        manager = WorkerManager()
        
        # Get job counts
        counts = queue.get_status()
        
        # Get active workers
        workers = manager.get_active_workers()
        
        print("=" * 50)
        print("QUEUE STATUS")
        print("=" * 50)
        print(f"\nJob States:")
        print(f"  Pending:    {counts['pending']}")
        print(f"  Processing: {counts['processing']}")
        print(f"  Completed:  {counts['completed']}")
        print(f"  Failed:     {counts['failed']}")
        print(f"  Dead (DLQ): {counts['dead']}")
        print(f"\nActive Workers: {len(workers)}")
        for worker in workers:
            print(f"  - PID {worker['pid']}")
        print("=" * 50)
        
        return 0
    except Exception as e:
        print(f"Error: {str(e)}")
        return 1

def cmd_list(args):
    """List jobs by state."""
    try:
        queue = JobQueue()
        jobs = queue.list_jobs(args.state)
        
        if args.state:
            print(f"\nJobs with state '{args.state}':")
        else:
            print("\nAll jobs:")
        
        print(format_job_table(jobs))
        print(f"\nTotal: {len(jobs)} job(s)")
        
        return 0
    except Exception as e:
        print(f"Error: {str(e)}")
        return 1

def cmd_dlq_list(args):
    """List jobs in DLQ."""
    try:
        queue = JobQueue()
        jobs = queue.get_dlq_jobs()
        
        print("\nDead Letter Queue:")
        print(format_job_table(jobs))
        print(f"\nTotal: {len(jobs)} job(s)")
        
        return 0
    except Exception as e:
        print(f"Error: {str(e)}")
        return 1

def cmd_dlq_retry(args):
    """Retry a job from DLQ."""
    try:
        queue = JobQueue()
        if queue.retry_dlq_job(args.job_id):
            print(f"✓ Job '{args.job_id}' moved back to queue")
            return 0
        else:
            print(f"Error: Job '{args.job_id}' not found in DLQ")
            return 1
    except Exception as e:
        print(f"Error: {str(e)}")
        return 1

def cmd_config_set(args):
    """Set configuration value."""
    try:
        config = Config()
        config.set(args.key, args.value)
        print(f"✓ Configuration updated: {args.key} = {args.value}")
        return 0
    except Exception as e:
        print(f"Error: {str(e)}")
        return 1

def cmd_config_get(args):
    """Get configuration value."""
    try:
        config = Config()
        if args.key:
            value = config.get(args.key)
            print(f"{args.key} = {value}")
        else:
            print("\nCurrent Configuration:")
            for key, value in config.get_all().items():
                print(f"  {key} = {value}")
        return 0
    except Exception as e:
        print(f"Error: {str(e)}")
        return 1

def main():
    """Main CLI entry point."""
    
    # --- CRITICAL FIX FOR WINDOWS ---
    # This line MUST be here and at the top of main()
    # It prevents new worker processes from re-running this script
    # and crashing.
    multiprocessing.freeze_support()
    # --- END FIX ---

    parser = argparse.ArgumentParser(
        prog="queuectl",
        description="CLI-based background job queue system"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Enqueue command
    enqueue_parser = subparsers.add_parser("enqueue", help="Add a new job to the queue")
    enqueue_parser.add_argument("job_json", help="Job JSON string")
    enqueue_parser.set_defaults(func=cmd_enqueue)
    
    # Worker commands
    worker_parser = subparsers.add_parser("worker", help="Worker management")
    worker_subparsers = worker_parser.add_subparsers(dest="worker_command")
    
    worker_start = worker_subparsers.add_parser("start", help="Start worker(s)")
    worker_start.add_argument("--count", type=int, default=1, help="Number of workers")
    worker_start.set_defaults(func=cmd_worker_start)
    
    worker_stop = worker_subparsers.add_parser("stop", help="Stop all workers")
    worker_stop.set_defaults(func=cmd_worker_stop)
    
    # Status command
    status_parser = subparsers.add_parser("status", help="Show queue status")
    status_parser.set_defaults(func=cmd_status)
    
    # List command
    list_parser = subparsers.add_parser("list", help="List jobs")
    list_parser.add_argument("--state", help="Filter by state")
    list_parser.set_defaults(func=cmd_list)
    
    # DLQ commands
    dlq_parser = subparsers.add_parser("dlq", help="Dead Letter Queue operations")
    dlq_subparsers = dlq_parser.add_subparsers(dest="dlq_command")
    
    dlq_list = dlq_subparsers.add_parser("list", help="List DLQ jobs")
    dlq_list.set_defaults(func=cmd_dlq_list)
    
    dlq_retry = dlq_subparsers.add_parser("retry", help="Retry a DLQ job")
    dlq_retry.add_argument("job_id", help="Job ID to retry")
    dlq_retry.set_defaults(func=cmd_dlq_retry)
    
    # Config commands
    config_parser = subparsers.add_parser("config", help="Configuration management")
    config_subparsers = config_parser.add_subparsers(dest="config_command")
    
    config_set = config_subparsers.add_parser("set", help="Set config value")
    config_set.add_argument("key", help="Configuration key")
    config_set.add_argument("value", help="Configuration value")
    config_set.set_defaults(func=cmd_config_set)
    
    config_get = config_subparsers.add_parser("get", help="Get config value")
    config_get.add_argument("key", nargs="?", help="Configuration key (optional)")
    config_get.set_defaults(func=cmd_config_get)
    
    # Parse arguments
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    if hasattr(args, "func"):
        return args.func(args)
    else:
        if args.command == "worker":
            worker_parser.print_help()
        elif args.command == "dlq":
            dlq_parser.print_help()
        elif args.command == "config":
            config_parser.print_help()
        return 1

if __name__ == "__main__":
    sys.exit(main())