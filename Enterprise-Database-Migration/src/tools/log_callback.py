"""
Log Callback System - Captures logs from agents for Streamlit UI.
"""

import sys
import time
import threading
from queue import Queue
from typing import Callable, Optional
from io import StringIO
from contextlib import contextmanager


class LogBuffer:
    """Thread-safe log buffer for capturing output."""
    
    def __init__(self):
        self._logs = []
        self._lock = threading.Lock()
        self._callbacks = []
    
    def add(self, message: str, level: str = "info"):
        """Add a log message."""
        timestamp = time.strftime("%H:%M:%S")
        icons = {"info": "ℹ️", "success": "✅", "warning": "⚠️", "error": "❌"}
        formatted = f"[{timestamp}] {icons.get(level, '•')} {message}"
        
        with self._lock:
            self._logs.append(formatted)
            for callback in self._callbacks:
                try:
                    callback(formatted)
                except Exception:
                    pass
    
    def get_all(self):
        """Get all logs."""
        with self._lock:
            return list(self._logs)
    
    def get_recent(self, n: int = 50):
        """Get recent n logs."""
        with self._lock:
            return list(self._logs[-n:])
    
    def clear(self):
        """Clear all logs."""
        with self._lock:
            self._logs = []
    
    def register_callback(self, callback: Callable[[str], None]):
        """Register a callback for new logs."""
        with self._lock:
            self._callbacks.append(callback)
    
    def unregister_callback(self, callback: Callable[[str], None]):
        """Unregister a callback."""
        with self._lock:
            if callback in self._callbacks:
                self._callbacks.remove(callback)


class StdoutCapture:
    """Captures stdout and sends to log buffer."""
    
    def __init__(self, log_buffer: LogBuffer, original_stdout=None):
        self.log_buffer = log_buffer
        self.original_stdout = original_stdout or sys.stdout
        self._buffer = ""
    
    def write(self, text: str):
        """Capture write and send to buffer."""
        # Also write to original stdout
        self.original_stdout.write(text)
        
        # Buffer until we get a newline
        self._buffer += text
        
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            if line.strip():
                # Determine level from content
                level = "info"
                if "✅" in line or "Success" in line:
                    level = "success"
                elif "⚠️" in line or "Warning" in line or "warning" in line:
                    level = "warning"
                elif "❌" in line or "Error" in line or "error" in line or "failed" in line:
                    level = "error"
                
                # Clean up the line (remove existing timestamps if present)
                clean_line = line.strip()
                if clean_line.startswith("[") and "]" in clean_line[:15]:
                    # Remove existing timestamp
                    clean_line = clean_line.split("]", 1)[-1].strip()
                
                # Remove emoji duplicates 
                for emoji in ["ℹ️", "✅", "⚠️", "❌"]:
                    if clean_line.startswith(emoji):
                        clean_line = clean_line[len(emoji):].strip()
                
                self.log_buffer.add(clean_line, level)
    
    def flush(self):
        """Flush the buffer."""
        self.original_stdout.flush()


# Global log buffer singleton
_log_buffer: Optional[LogBuffer] = None


def get_log_buffer() -> LogBuffer:
    """Get the global log buffer."""
    global _log_buffer
    if _log_buffer is None:
        _log_buffer = LogBuffer()
    return _log_buffer


def reset_log_buffer():
    """Reset the global log buffer."""
    global _log_buffer
    _log_buffer = LogBuffer()


@contextmanager
def capture_stdout():
    """Context manager to capture stdout to log buffer."""
    log_buffer = get_log_buffer()
    original_stdout = sys.stdout
    capture = StdoutCapture(log_buffer, original_stdout)
    
    sys.stdout = capture
    try:
        yield log_buffer
    finally:
        sys.stdout = original_stdout


def log(message: str, level: str = "info"):
    """Log a message to the global buffer."""
    get_log_buffer().add(message, level)
