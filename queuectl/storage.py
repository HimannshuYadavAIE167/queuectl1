import json
import os
import time
import contextlib
from pathlib import Path
from typing import List, Optional
from .models import Job

class Storage:
    """Handles persistent storage of jobs using JSON files with file locking."""

    def __init__(self, data_dir="data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.jobs_file = self.data_dir / "jobs.json"
        self.dlq_file = self.data_dir / "dlq.json"
        self.locks_file = self.data_dir / "locks.json"
        
        # --- NEW: Define lock files for cross-platform locking ---
        self.jobs_lock_file = self.data_dir / "jobs.json.lock"
        self.dlq_lock_file = self.data_dir / "dlq.json.lock"
        self.locks_lock_file = self.data_dir / "locks.json.lock"

        # Initialize files if they don't exist
        for file, lock_file in [
            (self.jobs_file, self.jobs_lock_file), 
            (self.dlq_file, self.dlq_lock_file), 
            (self.locks_file, self.locks_lock_file)
        ]:
            if not file.exists():
                self._write_json(file, {}, lock_file)
    
    @contextlib.contextmanager
    def _get_file_lock(self, lock_file_path):
        """
        A simple, cross-platform spin lock using an atomic lock file.
        """
        while True:
            try:
                # Try to atomically create the lock file
                fd = os.open(lock_file_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                # If successful, we have the lock
                os.close(fd)
                break
            except FileExistsError:
                # File already exists, wait and retry
                time.sleep(0.01)
            except PermissionError:
                # Handle potential permission issues, e.g. file being deleted
                time.sleep(0.01)
        
        try:
            # We have the lock, yield to the caller
            yield
        finally:
            # Always release the lock
            try:
                os.remove(lock_file_path)
            except FileNotFoundError:
                pass # Lock was already released

    @contextlib.contextmanager
    def _atomic_read_modify_write(self, file_path, lock_file_path):
        """
        Provides an atomic read-modify-write operation on a JSON file
        using the simple file lock.
        """
        with self._get_file_lock(lock_file_path):
            try:
                # Read current data
                with open(file_path, 'r') as f:
                    data = f.read()
                json_data = json.loads(data) if data else {}
            except FileNotFoundError:
                json_data = {}

            # Yield data to be modified by the caller
            yield json_data
            
            # Write new data
            with open(file_path, 'w') as f:
                json.dump(json_data, f, indent=2)

    def _read_json(self, file_path, lock_file_path):
        """Read JSON file with the file lock."""
        with self._get_file_lock(lock_file_path):
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                return data
            except (FileNotFoundError, json.JSONDecodeError):
                return {}

    def _write_json(self, file_path, data, lock_file_path):
        """Write JSON file with file lock (for init)."""
        with self._get_file_lock(lock_file_path):
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)

    def save_job(self, job: Job):
        """Save or update a job."""
        with self._atomic_read_modify_write(self.jobs_file, self.jobs_lock_file) as jobs:
            jobs[job.id] = job.to_dict()
    
    def get_job(self, job_id: str) -> Optional[Job]:
        """Get a job by ID."""
        jobs = self._read_json(self.jobs_file, self.jobs_lock_file)
        if job_id in jobs:
            return Job.from_dict(jobs[job_id])
        return None
    
    def get_all_jobs(self) -> List[Job]:
        """Get all jobs."""
        jobs = self._read_json(self.jobs_file, self.jobs_lock_file)
        return [Job.from_dict(job_data) for job_data in jobs.values()]
    
    def get_jobs_by_state(self, state: str) -> List[Job]:
        """Get jobs by state."""
        jobs = self.get_all_jobs()
        return [job for job in jobs if job.state == state]
    
    def delete_job(self, job_id: str):
        """Delete a job."""
        with self._atomic_read_modify_write(self.jobs_file, self.jobs_lock_file) as jobs:
            if job_id in jobs:
                del jobs[job_id]
    
    def move_to_dlq(self, job: Job):
        """Move a job to the Dead Letter Queue."""
        # Save to DLQ
        with self._atomic_read_modify_write(self.dlq_file, self.dlq_lock_file) as dlq:
            job.state = "dead"
            job.update_timestamp()
            dlq[job.id] = job.to_dict()
        
        # Remove from main queue
        self.delete_job(job.id)
    
    def get_dlq_jobs(self) -> List[Job]:
        """Get all jobs in the DLQ."""
        dlq = self._read_json(self.dlq_file, self.dlq_lock_file)
        return [Job.from_dict(job_data) for job_data in dlq.values()]
    
    def remove_from_dlq(self, job_id: str):
        """Remove a job from DLQ."""
        with self._atomic_read_modify_write(self.dlq_file, self.dlq_lock_file) as dlq:
            if job_id in dlq:
                del dlq[job_id]
    
    def acquire_lock(self, job_id: str, worker_id: str) -> bool:
        """Acquire a lock for a job."""
        with self._atomic_read_modify_write(self.locks_file, self.locks_lock_file) as locks:
            if job_id not in locks:
                locks[job_id] = worker_id
                return True
            return False
    
    def release_lock(self, job_id: str):
        """Release a lock for a job."""
        with self._atomic_read_modify_write(self.locks_file, self.locks_lock_file) as locks:
            if job_id in locks:
                del locks[job_id]
    
    def get_job_counts(self):
        """Get count of jobs by state."""
        jobs = self.get_all_jobs()
        dlq_jobs = self.get_dlq_jobs()
        counts = {
            "pending": 0,
            "processing": 0,
            "completed": 0,
            "failed": 0,
            "dead": len(dlq_jobs)
        }
        for job in jobs:
            if job.state in counts:
                counts[job.state] += 1
        return counts