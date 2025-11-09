import json
from pathlib import Path

class Config:
    """Manages system configuration."""
    
    DEFAULT_CONFIG = {
        "max_retries": 3,
        "backoff_base": 2,
        "data_dir": "data"
    }
    
    def __init__(self, config_file="data/config.json"):
        self.config_file = Path(config_file)
        self.config_file.parent.mkdir(exist_ok=True)
        
        # Load or create config
        if self.config_file.exists():
            self.config = self._load_config()
            # --- FIX: Ensure defaults are present ---
            config_changed = False
            for key, value in self.DEFAULT_CONFIG.items():
                if key not in self.config:
                    self.config[key] = value
                    config_changed = True
            if config_changed:
                self._save_config()
            # --- END FIX ---
        else:
            self.config = self.DEFAULT_CONFIG.copy()
            self._save_config()
    
    def _load_config(self):
        """Load configuration from file."""
        with open(self.config_file, 'r') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return self.DEFAULT_CONFIG.copy() # Handle empty/corrupt file
    
    def _save_config(self):
        """Save configuration to file."""
        with open(self.config_file, 'w') as f:
            json.dump(self.config, f, indent=2)
    
    def get(self, key, default=None):
        """Get a configuration value."""
        # --- FIX: Normalize key ---
        key = key.replace("-", "_")
        # --- END FIX ---
        return self.config.get(key, default)
    
    def set(self, key, value):
        """Set a configuration value."""
        # --- FIX: Normalize key ---
        key = key.replace("-", "_")
        # --- END FIX ---
        
        # Type conversion
        if key in ["max_retries", "backoff_base"]:
            value = int(value)
        
        self.config[key] = value
        self._save_config()
    
    def get_all(self):
        """Get all configuration values."""
        return self.config.copy()