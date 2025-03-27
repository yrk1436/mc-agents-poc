from typing import Dict, List, Optional
import sqlite3
from datetime import datetime
from pathlib import Path
from loguru import logger
import json

class ContextManager:
    """Unified context manager for handling both user and chat thread contexts using SQLite.
    
    This class manages:
    1. User-specific contexts (preferences, history across sessions)
    2. Thread-specific contexts (individual chat sessions)
    3. Combined history and context for interactions
    """
    
    def __init__(self, db_path: str = "data/context/context.db"):
        """Initialize the context manager.
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
    
    def _init_db(self):
        """Initialize the SQLite database with required tables"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Create user contexts table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_contexts (
                    user_id TEXT PRIMARY KEY,
                    context_data TEXT NOT NULL,
                    last_updated TIMESTAMP NOT NULL
                )
            """)
            
            # Create thread contexts table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS thread_contexts (
                    thread_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    context_data TEXT NOT NULL,
                    last_updated TIMESTAMP NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES user_contexts(user_id)
                )
            """)
            
            # Create interaction history table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS interaction_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    thread_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    question TEXT NOT NULL,
                    response TEXT NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    FOREIGN KEY (thread_id) REFERENCES thread_contexts(thread_id),
                    FOREIGN KEY (user_id) REFERENCES user_contexts(user_id)
                )
            """)
            
            conn.commit()
    
    def _dict_to_json(self, data: dict) -> str:
        """Convert dictionary to JSON string"""
        return json.dumps(data)
    
    def _json_to_dict(self, data: str) -> dict:
        """Convert JSON string to dictionary"""
        return json.loads(data) if data else {}
    
    def save_user_context(self, user_id: str, context: dict):
        """Save context that persists across all threads for a user.
        
        Args:
            user_id: Unique identifier for the user
            context: Context data to save
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            context_json = self._dict_to_json(context)
            timestamp = datetime.now().isoformat()
            
            cursor.execute("""
                INSERT OR REPLACE INTO user_contexts (user_id, context_data, last_updated)
                VALUES (?, ?, ?)
            """, (user_id, context_json, timestamp))
            
            conn.commit()
            logger.info(f"Updated context for user {user_id}")
    
    def save_thread_context(self, thread_id: str, user_id: str, context: dict):
        """Save context specific to a conversation thread.
        
        Args:
            thread_id: Unique identifier for the chat thread
            user_id: Unique identifier for the user
            context: Context data to save
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            context_json = self._dict_to_json(context)
            timestamp = datetime.now().isoformat()
            
            cursor.execute("""
                INSERT OR REPLACE INTO thread_contexts (thread_id, user_id, context_data, last_updated)
                VALUES (?, ?, ?, ?)
            """, (thread_id, user_id, context_json, timestamp))
            
            conn.commit()
            logger.info(f"Updated context for thread {thread_id}")
    
    def get_context(self, user_id: str, thread_id: str) -> dict:
        """Retrieve combined context for the current interaction.
        
        Args:
            user_id: Unique identifier for the user
            thread_id: Unique identifier for the chat thread
            
        Returns:
            Dict containing combined user and thread context
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Get user context
            cursor.execute("""
                SELECT context_data FROM user_contexts WHERE user_id = ?
            """, (user_id,))
            user_context_row = cursor.fetchone()
            user_context = self._json_to_dict(user_context_row[0]) if user_context_row else {}
            
            # Get thread context
            cursor.execute("""
                SELECT context_data FROM thread_contexts WHERE thread_id = ?
            """, (thread_id,))
            thread_context_row = cursor.fetchone()
            thread_context = self._json_to_dict(thread_context_row[0]) if thread_context_row else {}
            
            return {
                "user_context": user_context,
                "thread_context": thread_context,
                "combined_history": {
                    **user_context.get("preferences", {}),
                    **thread_context.get("history", {})
                }
            }
    
    def update_context(self, thread_id: str, user_id: str, updates: Dict) -> None:
        """Update specific fields in a thread's context.
        
        Args:
            thread_id: Unique identifier for the chat thread
            user_id: Unique identifier for the user
            updates: Dictionary of fields to update
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Get existing context
            cursor.execute("""
                SELECT context_data FROM thread_contexts WHERE thread_id = ?
            """, (thread_id,))
            row = cursor.fetchone()
            
            if row:
                context = self._json_to_dict(row[0])
                context.update(updates)
                context_json = self._dict_to_json(context)
                timestamp = datetime.now().isoformat()
                
                cursor.execute("""
                    UPDATE thread_contexts 
                    SET context_data = ?, last_updated = ?
                    WHERE thread_id = ?
                """, (context_json, timestamp, thread_id))
            else:
                # Create new context if it doesn't exist
                context_json = self._dict_to_json(updates)
                timestamp = datetime.now().isoformat()
                
                cursor.execute("""
                    INSERT INTO thread_contexts (thread_id, user_id, context_data, last_updated)
                    VALUES (?, ?, ?, ?)
                """, (thread_id, user_id, context_json, timestamp))
            
            conn.commit()
            logger.info(f"Updated context for thread {thread_id}")
    
    def list_threads(self, user_id: Optional[str] = None) -> List[str]:
        """List all thread IDs, optionally filtered by user_id.
        
        Args:
            user_id: Optional user ID to filter threads
            
        Returns:
            List of thread IDs
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            if user_id:
                cursor.execute("""
                    SELECT thread_id FROM thread_contexts WHERE user_id = ?
                """, (user_id,))
            else:
                cursor.execute("SELECT thread_id FROM thread_contexts")
            
            return [row[0] for row in cursor.fetchall()]
    
    def delete_thread(self, thread_id: str) -> bool:
        """Delete a chat thread's context.
        
        Args:
            thread_id: Unique identifier for the chat thread
            
        Returns:
            True if thread was deleted, False if not found
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Delete thread context
            cursor.execute("DELETE FROM thread_contexts WHERE thread_id = ?", (thread_id,))
            
            # Delete associated interaction history
            cursor.execute("DELETE FROM interaction_history WHERE thread_id = ?", (thread_id,))
            
            conn.commit()
            was_deleted = cursor.rowcount > 0
            
            if was_deleted:
                logger.info(f"Deleted context for thread {thread_id}")
            return was_deleted
    
    def update_interaction(self, user_id: str, thread_id: str, question: str, response: str):
        """Update context with the latest interaction.
        
        Args:
            user_id: Unique identifier for the user
            thread_id: Unique identifier for the chat thread
            question: The user's question
            response: The system's response
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            timestamp = datetime.now().isoformat()
            
            # Add to interaction history
            cursor.execute("""
                INSERT INTO interaction_history 
                (thread_id, user_id, question, response, timestamp)
                VALUES (?, ?, ?, ?, ?)
            """, (thread_id, user_id, question, response, timestamp))
            
            # Update thread context with latest interaction
            cursor.execute("""
                SELECT context_data FROM thread_contexts WHERE thread_id = ?
            """, (thread_id,))
            row = cursor.fetchone()
            
            if row:
                context = self._json_to_dict(row[0])
                if "history" not in context:
                    context["history"] = []
                context["history"].append({
                    "timestamp": timestamp,
                    "question": question,
                    "response": response
                })
                context_json = self._dict_to_json(context)
                
                cursor.execute("""
                    UPDATE thread_contexts 
                    SET context_data = ?, last_updated = ?
                    WHERE thread_id = ?
                """, (context_json, timestamp, thread_id))
            
            conn.commit()
            logger.info(f"Updated interaction history for user {user_id} in thread {thread_id}")
