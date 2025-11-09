import os
import signal
import subprocess
import time
import uuid
from pathlib import Path
from .queue import JobQueue
from .config import Config
import multiprocessing
import shutil  

class Worker:
    """Worker process that executes jobs from the queue."""
    
    def __init__(self, worker_id=None):
        self.worker_id = worker_id or str(uuid.uuid4())[:8]
        self.queue = JobQueue()
        self.running = False
        self.current_job = None
        
        # Get data_dir from the queue's storage config
        self.data_dir = self.queue.storage.data_dir 
        self.pid_file = self.data_dir / f"worker_{self.worker_id}.pid"
        
    def start(self):
        """Start the worker process."""
        # Write PID file
        self.pid_file.parent.mkdir(exist_ok=True)
        with open(self.pid_file, 'w') as f:
            f.write(str(os.getpid()))
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._shutdown_handler)
        signal.signal(signal.SIGINT, self._shutdown_handler)
        
        self.running = True
        print(f"[Worker {self.worker_id}] Started (PID: {os.getpid()})")
        
        try:
            while self.running:
                job = self.queue.get_next_job(self.worker_id)
                
                if job:
                    self.current_job = job
                    self._execute_job(job)
                    self.current_job = None
                else:
                    # No jobs available, sleep briefly
                    time.sleep(1)
        finally:
            self._cleanup()
    
    def _execute_job(self, job):
        """Execute a job."""
        print(f"[Worker {self.worker_id}] Processing job {job.id}: {job.command}")
        
        # Mark as processing
        self.queue.mark_processing(job.id)
        
        # --- MODIFIED BLOCK ---
        # Revert to the simple version.
        # Now that bash.exe is in your system PATH, this will work.
        executable = None
        if os.name == 'nt':
            executable = "bash.exe" 
        # --- END MODIFIED BLOCK ---
        
        try:
            # Execute the command
            result = subprocess.run(
                job.command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=300,  # 5 minute timeout
                # Add CREATE_NO_WINDOW flag for Windows to hide shells
                creationflags=0x08000000 if os.name == 'nt' else 0,
                executable=executable  # <-- Pass "bash.exe"
            )
            
            if result.returncode == 0:
                print(f"[Worker {self.worker_id}] Job {job.id} completed successfully")
                self.queue.mark_completed(job.id)
            else:
                error_msg = f"Command failed with exit code {result.returncode}"
                if result.stderr:
                    error_msg += f": {result.stderr[:200]}"
                print(f"[Worker {self.worker_id}] Job {job.id} failed: {error_msg}")
                self.queue.mark_failed(job.id, error_msg)
                
        except subprocess.TimeoutExpired:
            error_msg = "Job execution timeout (5 minutes)"
            print(f"[Worker {self.worker_id}] Job {job.id} timed out")
            self.queue.mark_failed(job.id, error_msg)
            
        except Exception as e:
            error_msg = f"Execution error: {str(e)}"
            print(f"[Worker {self.worker_id}] Job {job.id} error: {error_msg}")
            self.queue.mark_failed(job.id, error_msg)
    
    def _shutdown_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        print(f"\n[Worker {self.worker_id}] Shutdown signal received...")
        if self.current_job:
            print(f"[Worker {self.worker_id}] Finishing current job {self.current_job.id}...")
        self.running = False
    
    def _cleanup(self):
        """Cleanup worker resources."""
        if self.pid_file.exists():
            try:
                self.pid_file.unlink()
            except PermissionError:
                pass # May fail on Windows if process is terminating
        print(f"[Worker {self.worker_id}] Stopped")


class WorkerManager:
    """Manages multiple worker processes."""
    
    def __init__(self):
        self.config = Config()
        self.data_dir = Path(self.config.get("data_dir"))
        self.data_dir.mkdir(exist_ok=True)

    @staticmethod
    def _launch_worker():
        """
        Static method to be the target of the new process.
        Creates and starts a worker.
        """
        try:
            worker = Worker()
            worker.start()
        except KeyboardInterrupt:
            pass # Handle Ctrl+C in child

    def start_workers(self, count=1):
        """Start multiple worker processes using multiprocessing."""
        processes = []
        for i in range(count):
            p = multiprocessing.Process(target=self._launch_worker)
            p.start() 
            print(f"Started worker process (PID: {p.pid})")
            processes.append(p)
        
        return processes
    
    def get_active_workers(self):
        """Get list of active worker PIDs."""
        workers = []
        for pid_file in self.data_dir.glob("worker_*.pid"):
            try:
                with open(pid_file, 'r') as f:
                    pid = int(f.read().strip())
                
                # --- Cross-platform process check ---
                if os.name == 'nt': # Windows
                    # Check if process exists by querying tasklist
                    result = subprocess.run(
                        ['tasklist', '/FI', f'PID eq {pid}'],
                        capture_output=True, text=True,
                        creationflags=0x08000000 # CREATE_NO_WINDOW
                    )
                    if f" {pid} " in result.stdout:
                        workers.append({"pid": pid, "file": pid_file.name})
                    else:
                        raise ProcessLookupError # Not found
                else: # Unix/Linux/macOS
                    os.kill(pid, 0) # Check if process is still running
                    workers.append({"pid": pid, "file": pid_file.name})
            
            except (ProcessLookupError, ValueError, FileNotFoundError, PermissionError):
                # Process not running or PID file is stale/empty
                try:
                    pid_file.unlink()
                except (FileNotFoundError, PermissionError):
                    pass # Already gone or in use
        return workers

    def stop_workers(self):
        """Stop all running workers gracefully."""
        workers = self.get_active_workers()
        if not workers:
            print("No active workers found")
            return
        
        for worker in workers:
            try:
                print(f"Stopping worker (PID: {worker['pid']})...")
                # Send SIGTERM (graceful shutdown)
                os.kill(worker['pid'], signal.SIGTERM)
            except (ProcessLookupError, PermissionError):
                print(f"Worker {worker['pid']} already stopped or access denied")
        
        # Wait for workers to finish
        time.sleep(2)
        
        # Check if any workers are still running
        remaining = self.get_get_active_workers()
        if remaining:
            print(f"{len(remaining)} workers still running (finishing their jobs)")
        else:
            print("All workers stopped")