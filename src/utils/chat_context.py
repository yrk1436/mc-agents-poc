import json
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

class ChatContextManager:
    def __init__(self, context_dir: str = "chat_context"):
        """Initialize chat context manager.
        
        Args:
            context_dir: Directory to store chat context files
        """
        self.context_dir = Path(context_dir)
        self.context_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_context_file(self, thread_id: str) -> Path:
        """Get the path to the context file for a thread."""
        return self.context_dir / f"{thread_id}.json"
    
    def save_context(self, thread_id: str, user_id: str, context: Dict) -> None:
        """Save context for a chat thread.
        
        Args:
            thread_id: Unique identifier for the chat window
            user_id: ID of the user chatting
            context: Context data to save
        """
        context_file = self._get_context_file(thread_id)
        
        # Add metadata
        data = {
            "thread_id": thread_id,
            "user_id": user_id,
            "last_updated": datetime.now().isoformat(),
            "context": context
        }
        
        with open(context_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    def get_context(self, thread_id: str) -> Optional[Dict]:
        """Get context for a chat thread.
        
        Args:
            thread_id: Unique identifier for the chat window
            
        Returns:
            Dict containing thread context if found, None otherwise
        """
        context_file = self._get_context_file(thread_id)
        
        if context_file.exists():
            with open(context_file, 'r') as f:
                return json.load(f)
        return None
    
    def update_context(self, thread_id: str, updates: Dict) -> None:
        """Update specific fields in a thread's context.
        
        Args:
            thread_id: Unique identifier for the chat window
            updates: Dictionary of fields to update
        """
        context = self.get_context(thread_id)
        if context:
            context["context"].update(updates)
            context["last_updated"] = datetime.now().isoformat()
            
            with open(self._get_context_file(thread_id), 'w') as f:
                json.dump(context, f, indent=2)
    
    def list_threads(self, user_id: Optional[str] = None) -> List[str]:
        """List all thread IDs, optionally filtered by user_id.
        
        Args:
            user_id: Optional user ID to filter threads
            
        Returns:
            List of thread IDs
        """
        threads = []
        for file in self.context_dir.glob("*.json"):
            with open(file, 'r') as f:
                data = json.load(f)
                if user_id is None or data["user_id"] == user_id:
                    threads.append(data["thread_id"])
        return threads
    
    def delete_thread(self, thread_id: str) -> bool:
        """Delete a chat thread's context.
        
        Args:
            thread_id: Unique identifier for the chat window
            
        Returns:
            True if thread was deleted, False if not found
        """
        context_file = self._get_context_file(thread_id)
        if context_file.exists():
            context_file.unlink()
            return True
        return False
