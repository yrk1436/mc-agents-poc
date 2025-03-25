from typing import Dict, Optional
import json
from datetime import datetime
from pathlib import Path
from loguru import logger

class ContextManager:
    def __init__(self, storage_dir: str = "data/context"):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.thread_contexts: Dict[str, dict] = {}
        self.user_contexts: Dict[str, dict] = {}
        self._load_contexts()

    def _load_contexts(self):
        """Load existing contexts from storage"""
        thread_file = self.storage_dir / "thread_contexts.json"
        user_file = self.storage_dir / "user_contexts.json"

        if thread_file.exists():
            with open(thread_file, "r") as f:
                self.thread_contexts = json.load(f)

        if user_file.exists():
            with open(user_file, "r") as f:
                self.user_contexts = json.load(f)

    def _save_contexts(self):
        """Persist contexts to storage"""
        with open(self.storage_dir / "thread_contexts.json", "w") as f:
            json.dump(self.thread_contexts, f)

        with open(self.storage_dir / "user_contexts.json", "w") as f:
            json.dump(self.user_contexts, f)

    def save_thread_context(self, thread_id: str, context: dict):
        """Save context specific to a conversation thread"""
        if thread_id not in self.thread_contexts:
            self.thread_contexts[thread_id] = {}

        self.thread_contexts[thread_id].update({
            **context,
            "last_updated": datetime.now().isoformat()
        })
        self._save_contexts()
        logger.info(f"Updated context for thread {thread_id}")

    def save_user_context(self, user_id: str, context: dict):
        """Save context that persists across all threads for a user"""
        if user_id not in self.user_contexts:
            self.user_contexts[user_id] = {}

        self.user_contexts[user_id].update({
            **context,
            "last_updated": datetime.now().isoformat()
        })
        self._save_contexts()
        logger.info(f"Updated context for user {user_id}")

    def get_context(self, user_id: str, thread_id: str) -> dict:
        """Retrieve combined context for the current interaction"""
        user_context = self.user_contexts.get(user_id, {})
        thread_context = self.thread_contexts.get(thread_id, {})

        return {
            "user_context": user_context,
            "thread_context": thread_context,
            "combined_history": {
                **user_context.get("preferences", {}),
                **thread_context.get("history", {})
            }
        }

    def clear_thread_context(self, thread_id: str):
        """Clear context for a specific thread"""
        if thread_id in self.thread_contexts:
            del self.thread_contexts[thread_id]
            self._save_contexts()
            logger.info(f"Cleared context for thread {thread_id}")

    def update_interaction(self, user_id: str, thread_id: str, question: str, response: str):
        """Update context with the latest interaction"""
        timestamp = datetime.now().isoformat()
        
        # Update thread history
        if thread_id not in self.thread_contexts:
            self.thread_contexts[thread_id] = {"history": []}
            
        self.thread_contexts[thread_id]["history"].append({
            "timestamp": timestamp,
            "question": question,
            "response": response
        })
        
        # Update user's last interaction
        if user_id not in self.user_contexts:
            self.user_contexts[user_id] = {}
            
        self.user_contexts[user_id]["last_interaction"] = timestamp
        self._save_contexts()
