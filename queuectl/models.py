from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional
import json

@dataclass
class Job:
    """Represents a background job in the queue."""
    id: str
    command: str
    state: str = "pending"  # pending, processing, completed, failed, dead
    attempts: int = 0
    max_retries: int = 3
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    last_error: Optional[str] = None
    next_retry_at: Optional[str] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow().isoformat() + "Z"
        if self.updated_at is None:
            self.updated_at = datetime.utcnow().isoformat() + "Z"
    
    def to_dict(self):
        """Convert job to dictionary."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data):
        """Create job from dictionary."""
        return cls(**data)
    
    def update_timestamp(self):
        """Update the updated_at timestamp."""
        self.updated_at = datetime.utcnow().isoformat() + "Z"