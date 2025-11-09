from datetime import datetime, timedelta
from typing import Optional
from .models import Job
from .storage import Storage
from .config import Config

class JobQueue:
    """Manages the job queue operations."""
    
    def __init__(self):
        self.config = Config()
        self.storage = Storage(self.config.get("data_dir"))
    
    def enqueue(self, job_data: dict) -> Job:
        """Add a new job to the queue."""
        # Use max_retries from config if not specified
        if "max_retries" not in job_data:
            job_data["max_retries"] = self.config.get("max_retries")
        
        job = Job.from_dict(job_data)
        self.storage.save_job(job)
        return job
    
    def get_next_job(self, worker_id: str) -> Optional[Job]:
        """Get the next pending job that's ready to run."""
        pending_jobs = self.storage.get_jobs_by_state("pending")
        
        # Also check failed jobs that are ready for retry
        failed_jobs = self.storage.get_jobs_by_state("failed")
        for job in failed_jobs:
            if job.next_retry_at:
                retry_time = datetime.fromisoformat(job.next_retry_at.replace('Z', '+00:00'))
                if datetime.now(retry_time.tzinfo) >= retry_time:
                    pending_jobs.append(job)
        
        # Sort by created_at (FIFO)
        pending_jobs.sort(key=lambda j: j.created_at)
        
        # Try to acquire lock for the first available job
        for job in pending_jobs:
            if self.storage.acquire_lock(job.id, worker_id):
                return job
        
        return None
    
    def mark_processing(self, job_id: str):
        """Mark a job as processing."""
        job = self.storage.get_job(job_id)
        if job:
            job.state = "processing"
            job.attempts += 1
            job.update_timestamp()
            self.storage.save_job(job)
    
    def mark_completed(self, job_id: str):
        """Mark a job as completed."""
        job = self.storage.get_job(job_id)
        if job:
            job.state = "completed"
            job.update_timestamp()
            self.storage.save_job(job)
            self.storage.release_lock(job_id)
    
    def mark_failed(self, job_id: str, error: str):
        """Mark a job as failed and schedule retry or move to DLQ."""
        job = self.storage.get_job(job_id)
        if not job:
            return
        
        job.last_error = error
        job.update_timestamp()
        
        # Check if we should retry or move to DLQ
        if job.attempts >= job.max_retries:
            # Move to DLQ
            self.storage.move_to_dlq(job)
        else:
            # Schedule retry with exponential backoff
            backoff_base = self.config.get("backoff_base")
            delay_seconds = backoff_base ** job.attempts
            retry_time = datetime.utcnow() + timedelta(seconds=delay_seconds)
            job.next_retry_at = retry_time.isoformat() + "Z"
            job.state = "failed"
            self.storage.save_job(job)
        
        self.storage.release_lock(job_id)
    
    def list_jobs(self, state: Optional[str] = None):
        """List all jobs or jobs by state."""
        if state:
            return self.storage.get_jobs_by_state(state)
        return self.storage.get_all_jobs()
    
    def get_status(self):
        """Get queue status summary."""
        return self.storage.get_job_counts()
    
    def get_dlq_jobs(self):
        """Get all jobs in the Dead Letter Queue."""
        return self.storage.get_dlq_jobs()
    
    def retry_dlq_job(self, job_id: str):
        """Retry a job from the DLQ."""
        dlq_jobs = self.storage.get_dlq_jobs()
        job = next((j for j in dlq_jobs if j.id == job_id), None)
        
        if job:
            # Reset job state
            job.state = "pending"
            job.attempts = 0
            job.last_error = None
            job.next_retry_at = None
            job.update_timestamp()
            
            # Move back to main queue
            self.storage.remove_from_dlq(job_id)
            self.storage.save_job(job)
            return True
        return False