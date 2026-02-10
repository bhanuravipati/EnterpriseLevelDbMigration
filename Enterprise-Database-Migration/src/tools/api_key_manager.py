"""
API Key Manager - Handles rotating API keys when rate-limited.
Supports multiple Groq API keys for fallback during rate limiting.
"""

import os
from typing import Optional


class APIKeyManager:
    """Manages multiple API keys with rotation on rate limits."""
    
    _instance: Optional["APIKeyManager"] = None
    
    def __init__(self):
        self.keys: list[str] = []
        self.current_index: int = 0
        self.failed_keys: set[str] = set()
        self._load_keys()
    
    def _load_keys(self):
        """Load API keys from environment variables."""
        # Primary key
        primary_key = os.getenv("GROQ_API_KEY", "")
        if primary_key:
            self.keys.append(primary_key)
        
        # Additional keys (GROQ_API_KEY_1, GROQ_API_KEY_2, etc.)
        for i in range(1, 10):
            key = os.getenv(f"GROQ_API_KEY_{i}", "")
            if key:
                self.keys.append(key)
        
        if not self.keys:
            raise ValueError("No GROQ_API_KEY found in environment")
        
        print(f"🔑 Loaded {len(self.keys)} API key(s)")
    
    @property
    def current_key(self) -> str:
        """Get the current active API key."""
        if not self.keys:
            raise ValueError("No API keys available")
        return self.keys[self.current_index]
    
    def rotate_key(self, reason: str = "rate_limited") -> bool:
        """
        Rotate to the next available API key.
        Returns True if rotation succeeded, False if all keys exhausted.
        """
        # Mark current key as failed
        self.failed_keys.add(self.keys[self.current_index])
        
        # Try to find next working key
        original_index = self.current_index
        
        for _ in range(len(self.keys)):
            self.current_index = (self.current_index + 1) % len(self.keys)
            
            if self.keys[self.current_index] not in self.failed_keys:
                print(f"🔄 Rotated to API key {self.current_index + 1}/{len(self.keys)} ({reason})")
                return True
            
            if self.current_index == original_index:
                break
        
        print(f"⚠️ All {len(self.keys)} API keys exhausted!")
        return False
    
    def reset_failed_keys(self):
        """Reset the failed keys set (e.g., at start of new migration)."""
        self.failed_keys.clear()
        self.current_index = 0
    
    def get_key_status(self) -> dict:
        """Get status of all keys."""
        return {
            "total_keys": len(self.keys),
            "current_index": self.current_index,
            "failed_count": len(self.failed_keys),
            "available_count": len(self.keys) - len(self.failed_keys)
        }


# Global singleton
_key_manager: Optional[APIKeyManager] = None


def get_api_key_manager() -> APIKeyManager:
    """Get or create the global API key manager."""
    global _key_manager
    if _key_manager is None:
        _key_manager = APIKeyManager()
    return _key_manager


def reset_api_key_manager():
    """Reset the API key manager (for new migration runs)."""
    global _key_manager
    if _key_manager:
        _key_manager.reset_failed_keys()
