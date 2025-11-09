import unittest
import os
import shutil
from queuectl.queue import JobQueue
from queuectl.models import Job

class TestJobQueue(unittest.TestCase):
    """Test cases for JobQueue."""
    
    def setUp(self):
        """Set up test environment."""
        self.test_data_dir = "test_data"
        if os.path.exists(self.test_data_dir):
            shutil.rmtree(self.test_data_dir)
        os.makedirs(self.test_data_dir)
        
        # Temporarily change data directory
        os.environ['QUEUECTL_DATA_DIR'] = self.test_data_dir
        self.queue = JobQueue()
    
    def tearDown(self):
        """Clean up test environment."""
        if os.path.exists(self.test_data_dir):
            shutil.rmtree(self.test_data_dir)
    
    def test_enqueue_job(self):
        """Test enqueuing a job."""
        job_data = {
            "id": "test1",
            "command": "echo test"
        }
        job = self.queue.enqueue(job_data)
        
        self.assertEqual(job.id, "test1")
        self.assertEqual(job.command, "echo test")
        self.assertEqual(job.state, "pending")
        self.assertEqual(job.attempts, 0)
    
    def test_get_next_job(self):
        """Test getting next job from queue."""
        # Enqueue a job
        self.queue.enqueue({"id": "test1", "command": "echo test"})
        
        # Get next job
        job = self.queue.get_next_job("worker1")
        
        self.assertIsNotNone(job)
        self.assertEqual(job.id, "test1")
    
    def test_mark_completed(self):
        """Test marking job as completed."""
        # Enqueue and get job
        self.queue.enqueue({"id": "test1", "command": "echo test"})
        job = self.queue.get_next_job("worker1")
        self.queue.mark_processing(job.id)
        
        # Mark completed
        self.queue.mark_completed(job.id)
        
        # Verify state
        job = self.queue.storage.get_job(job.id)
        self.assertEqual(job.state, "completed")
    
    def test_retry_with_backoff(self):
        """Test job retry with exponential backoff."""
        # Enqueue job
        job_data = {"id": "test1", "command": "exit 1", "max_retries": 3}
        self.queue.enqueue(job_data)
        
        # Get and fail the job
        job = self.queue.get_next_job("worker1")
        self.queue.mark_processing(job.id)
        self.queue.mark_failed(job.id, "Command failed")
        
        # Verify it's marked as failed
        job = self.queue.storage.get_job(job.id)
        self.assertEqual(job.state, "failed")
        self.assertEqual(job.attempts, 1)
        self.assertIsNotNone(job.next_retry_at)
    
    def test_move_to_dlq(self):
        """Test moving job to DLQ after max retries."""
        # Enqueue job with max_retries=1
        job_data = {"id": "test1", "command": "exit 1", "max_retries": 1}
        self.queue.enqueue(job_data)
        
        # Fail it once
        job = self.queue.get_next_job("worker1")
        self.queue.mark_processing(job.id)
        self.queue.mark_failed(job.id, "Command failed")
        
        # Try again - should move to DLQ
        job = self.queue.storage.get_job(job.id)
        if job:  # If still in main queue
            self.queue.mark_processing(job.id)
            self.queue.mark_failed(job.id, "Command failed again")
        
        # Verify it's in DLQ
        dlq_jobs = self.queue.get_dlq_jobs()
        self.assertTrue(any(j.id == "test1" for j in dlq_jobs))
    
    def test_list_jobs_by_state(self):
        """Test listing jobs by state."""
        # Enqueue multiple jobs
        self.queue.enqueue({"id": "test1", "command": "echo 1"})
        self.queue.enqueue({"id": "test2", "command": "echo 2"})
        
        # Get pending jobs
        pending = self.queue.list_jobs("pending")
        self.assertEqual(len(pending), 2)
    
    def test_retry_dlq_job(self):
        """Test retrying a job from DLQ."""
        # Create a DLQ job
        job_data = {"id": "test1", "command": "exit 1", "max_retries": 1}
        self.queue.enqueue(job_data)
        
        job = self.queue.get_next_job("worker1")
        self.queue.mark_processing(job.id)
        self.queue.mark_failed(job.id, "Error")
        
        # Get the job again and fail to move to DLQ
        job = self.queue.storage.get_job(job.id)
        if job:
            self.queue.mark_processing(job.id)
            self.queue.mark_failed(job.id, "Error 2")
        
        # Retry from DLQ
        success = self.queue.retry_dlq_job("test1")
        self.assertTrue(success)
        
        # Verify it's back in pending
        job = self.queue.storage.get_job("test1")
        self.assertIsNotNone(job)
        self.assertEqual(job.state, "pending")
        self.assertEqual(job.attempts, 0)

if __name__ == '__main__':
    unittest.main()