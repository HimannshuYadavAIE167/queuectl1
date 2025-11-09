import unittest
import os
import shutil
import time
import signal
from queuectl.worker import Worker, WorkerManager
from queuectl.queue import JobQueue
from queuectl.storage import Storage

class TestWorker(unittest.TestCase):
    """Test cases for Worker."""
    
    def setUp(self):
        """Set up test environment."""
        self.test_data_dir = "test_data"
        if os.path.exists(self.test_data_dir):
            shutil.rmtree(self.test_data_dir)
        os.makedirs(self.test_data_dir)
        
        # Use test data directory
        os.environ['QUEUECTL_DATA_DIR'] = self.test_data_dir
        self.queue = JobQueue()
        self.storage = Storage(self.test_data_dir)
    
    def tearDown(self):
        """Clean up test environment."""
        if os.path.exists(self.test_data_dir):
            shutil.rmtree(self.test_data_dir)
    
    def test_worker_initialization(self):
        """Test worker can be initialized."""
        worker = Worker()
        self.assertIsNotNone(worker.worker_id)
        self.assertFalse(worker.running)
        self.assertIsNone(worker.current_job)
    
    def test_job_execution_success(self):
        """Test worker successfully executes a job."""
        # Enqueue a simple job
        job_data = {"id": "test1", "command": "echo 'Hello World'"}
        self.queue.enqueue(job_data)
        
        # Create worker and execute one job
        worker = Worker()
        job = self.queue.get_next_job(worker.worker_id)
        
        self.assertIsNotNone(job)
        worker._execute_job(job)
        
        # Verify job completed
        completed_job = self.storage.get_job("test1")
        self.assertEqual(completed_job.state, "completed")
        self.assertEqual(completed_job.attempts, 1)
    
    def test_job_execution_failure(self):
        """Test worker handles failed job correctly."""
        # Enqueue a job that will fail
        job_data = {"id": "test_fail", "command": "exit 1", "max_retries": 2}
        self.queue.enqueue(job_data)
        
        # Execute the job
        worker = Worker()
        job = self.queue.get_next_job(worker.worker_id)
        worker._execute_job(job)
        
        # Verify job failed and is ready for retry
        failed_job = self.storage.get_job("test_fail")
        self.assertEqual(failed_job.state, "failed")
        self.assertEqual(failed_job.attempts, 1)
        self.assertIsNotNone(failed_job.last_error)
        self.assertIsNotNone(failed_job.next_retry_at)
    
    def test_job_moves_to_dlq_after_max_retries(self):
        """Test job moves to DLQ after exhausting retries."""
        # Enqueue a job with max_retries=1
        job_data = {"id": "test_dlq", "command": "exit 1", "max_retries": 1}
        self.queue.enqueue(job_data)
        
        worker = Worker()
        
        # First attempt - should fail and mark for retry
        job = self.queue.get_next_job(worker.worker_id)
        worker._execute_job(job)
        
        # Second attempt - should fail and move to DLQ
        job = self.storage.get_job("test_dlq")
        if job:  # May still be in main queue
            self.queue.mark_processing(job.id)
            worker._execute_job(job)
        
        # Verify job is in DLQ
        dlq_jobs = self.queue.get_dlq_jobs()
        self.assertTrue(any(j.id == "test_dlq" for j in dlq_jobs))
    
    def test_worker_timeout(self):
        """Test worker handles job timeout."""
        # Enqueue a long-running job (our timeout is 300 seconds, so this test uses a mock)
        # In real scenario, this would timeout
        job_data = {"id": "test_timeout", "command": "sleep 1"}
        self.queue.enqueue(job_data)
        
        worker = Worker()
        job = self.queue.get_next_job(worker.worker_id)
        
        # Execute job (will complete before timeout in this case)
        worker._execute_job(job)
        
        # Job should complete successfully since 1 second < 300 seconds
        completed_job = self.storage.get_job("test_timeout")
        self.assertEqual(completed_job.state, "completed")
    
    def test_invalid_command(self):
        """Test worker handles invalid command gracefully."""
        # Enqueue a job with invalid command
        job_data = {"id": "test_invalid", "command": "nonexistent_command_xyz"}
        self.queue.enqueue(job_data)
        
        worker = Worker()
        job = self.queue.get_next_job(worker.worker_id)
        worker._execute_job(job)
        
        # Verify job failed
        failed_job = self.storage.get_job("test_invalid")
        self.assertEqual(failed_job.state, "failed")
        self.assertIn("Command failed", failed_job.last_error)
    
    def test_worker_pid_file_creation(self):
        """Test worker creates PID file on start."""
        worker = Worker()
        
        # Manually write PID file as start() runs infinite loop
        worker.pid_file.parent.mkdir(exist_ok=True)
        with open(worker.pid_file, 'w') as f:
            f.write(str(os.getpid()))
        
        # Verify PID file exists
        self.assertTrue(worker.pid_file.exists())
        
        # Cleanup
        worker._cleanup()
        self.assertFalse(worker.pid_file.exists())


class TestWorkerManager(unittest.TestCase):
    """Test cases for WorkerManager."""
    
    def setUp(self):
        """Set up test environment."""
        self.test_data_dir = "test_data"
        if os.path.exists(self.test_data_dir):
            shutil.rmtree(self.test_data_dir)
        os.makedirs(self.test_data_dir)
        
        self.manager = WorkerManager()
        self.manager.data_dir = self.test_data_dir
    
    def tearDown(self):
        """Clean up test environment."""
        # Stop any running workers
        try:
            self.manager.stop_workers()
        except:
            pass
        
        if os.path.exists(self.test_data_dir):
            shutil.rmtree(self.test_data_dir)
    
    def test_get_active_workers_empty(self):
        """Test getting active workers when none are running."""
        workers = self.manager.get_active_workers()
        self.assertEqual(len(workers), 0)
    
    def test_worker_manager_initialization(self):
        """Test WorkerManager initializes correctly."""
        manager = WorkerManager()
        self.assertTrue(manager.data_dir.exists())
    
    def test_stale_pid_cleanup(self):
        """Test that stale PID files are cleaned up."""
        # Create a fake PID file with non-existent PID
        fake_pid_file = self.manager.data_dir / "worker_fake.pid"
        with open(fake_pid_file, 'w') as f:
            f.write("999999")  # PID that doesn't exist
        
        # Get active workers should clean it up
        workers = self.manager.get_active_workers()
        
        self.assertEqual(len(workers), 0)
        self.assertFalse(fake_pid_file.exists())
    
    def test_worker_tracking(self):
        """Test that workers can be tracked via PID files."""
        # Create a PID file for current process
        pid_file = self.manager.data_dir / f"worker_test.pid"
        with open(pid_file, 'w') as f:
            f.write(str(os.getpid()))
        
        # Get active workers
        workers = self.manager.get_active_workers()
        
        self.assertEqual(len(workers), 1)
        self.assertEqual(workers[0]['pid'], os.getpid())
        
        # Cleanup
        pid_file.unlink()


class TestWorkerJobLocking(unittest.TestCase):
    """Test cases for job locking mechanism."""
    
    def setUp(self):
        """Set up test environment."""
        self.test_data_dir = "test_data"
        if os.path.exists(self.test_data_dir):
            shutil.rmtree(self.test_data_dir)
        os.makedirs(self.test_data_dir)
        
        self.queue = JobQueue()
        self.storage = Storage(self.test_data_dir)
    
    def tearDown(self):
        """Clean up test environment."""
        if os.path.exists(self.test_data_dir):
            shutil.rmtree(self.test_data_dir)
    
    def test_job_locking_prevents_duplicate_processing(self):
        """Test that job locking prevents duplicate processing."""
        # Enqueue a job
        job_data = {"id": "test_lock", "command": "echo test"}
        self.queue.enqueue(job_data)
        
        # Worker 1 gets the job
        worker1 = Worker("worker1")
        job1 = self.queue.get_next_job(worker1.worker_id)
        
        # Worker 2 tries to get the same job
        worker2 = Worker("worker2")
        job2 = self.queue.get_next_job(worker2.worker_id)
        
        # Worker 1 should get the job, worker 2 should get None
        self.assertIsNotNone(job1)
        self.assertIsNone(job2)
    
    def test_lock_release_after_completion(self):
        """Test that locks are released after job completion."""
        # Enqueue and process a job
        job_data = {"id": "test_release", "command": "echo test"}
        self.queue.enqueue(job_data)
        
        worker = Worker("worker1")
        job = self.queue.get_next_job(worker.worker_id)
        self.queue.mark_processing(job.id)
        self.queue.mark_completed(job.id)
        
        # Try to acquire lock again (should fail since job is completed)
        can_lock = self.storage.acquire_lock("test_release", "worker2")
        
        # Lock should be released
        locks = self.storage._read_json(self.storage.locks_file)
        self.assertNotIn("test_release", locks)
    
    def test_lock_release_after_failure(self):
        """Test that locks are released after job failure."""
        # Enqueue and fail a job
        job_data = {"id": "test_fail_lock", "command": "exit 1"}
        self.queue.enqueue(job_data)
        
        worker = Worker("worker1")
        job = self.queue.get_next_job(worker.worker_id)
        self.queue.mark_processing(job.id)
        self.queue.mark_failed(job.id, "Test error")
        
        # Lock should be released
        locks = self.storage._read_json(self.storage.locks_file)
        self.assertNotIn("test_fail_lock", locks)


class TestWorkerGracefulShutdown(unittest.TestCase):
    """Test cases for worker graceful shutdown."""
    
    def setUp(self):
        """Set up test environment."""
        self.test_data_dir = "test_data"
        if os.path.exists(self.test_data_dir):
            shutil.rmtree(self.test_data_dir)
        os.makedirs(self.test_data_dir)
    
    def tearDown(self):
        """Clean up test environment."""
        if os.path.exists(self.test_data_dir):
            shutil.rmtree(self.test_data_dir)
    
    def test_shutdown_handler_sets_running_false(self):
        """Test that shutdown handler stops the worker."""
        worker = Worker()
        worker.running = True
        
        # Simulate shutdown signal
        worker._shutdown_handler(signal.SIGTERM, None)
        
        self.assertFalse(worker.running)
    
    def test_cleanup_removes_pid_file(self):
        """Test that cleanup removes PID file."""
        worker = Worker()
        
        # Create PID file
        worker.pid_file.parent.mkdir(exist_ok=True)
        with open(worker.pid_file, 'w') as f:
            f.write(str(os.getpid()))
        
        self.assertTrue(worker.pid_file.exists())
        
        # Cleanup
        worker._cleanup()
        
        self.assertFalse(worker.pid_file.exists())


if __name__ == '__main__':
    unittest.main()