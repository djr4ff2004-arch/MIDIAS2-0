"""
User management module
Handles user data, statistics, and preferences
"""

import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class UserManager:
    """Manages user data and statistics"""
    
    def __init__(self, data_file: str = "users_data.json"):
        self.data_file = data_file
        self.users: Dict[int, Dict[str, Any]] = self._load_data()
    
    def _load_data(self) -> Dict[int, Dict[str, Any]]:
        """Load user data from file"""
        try:
            with open(self.data_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            return {}
        except Exception as e:
            logger.error(f"Error loading user data: {e}")
            return {}
    
    def _save_data(self):
        """Save user data to file"""
        try:
            with open(self.data_file, 'w', encoding='utf-8') as f:
                json.dump(self.users, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Error saving user data: {e}")
    
    def get_user(self, user_id: int) -> Dict[str, Any]:
        """Get user data, create if doesn't exist"""
        user_id_str = str(user_id)
        
        if user_id_str not in self.users:
            self.users[user_id_str] = {
                "id": user_id,
                "first_seen": datetime.now().isoformat(),
                "last_seen": datetime.now().isoformat(),
                "searches": 0,
                "language": "pt",
                "is_vip": False
            }
            self._save_data()
        
        return self.users[user_id_str]
    
    def update_user(self, user_id: int, **kwargs):
        """Update user data"""
        user = self.get_user(user_id)
        user.update(kwargs)
        user["last_seen"] = datetime.now().isoformat()
        self._save_data()
    
    def increment_searches(self, user_id: int):
        """Increment user's search count"""
        user = self.get_user(user_id)
        user["searches"] = user.get("searches", 0) + 1
        user["last_seen"] = datetime.now().isoformat()
        self._save_data()
    
    def set_language(self, user_id: int, language: str):
        """Set user's preferred language"""
        self.update_user(user_id, language=language)
    
    def get_language(self, user_id: int) -> str:
        """Get user's preferred language"""
        user = self.get_user(user_id)
        return user.get("language", "pt")
    
    def is_vip(self, user_id: int) -> bool:
        """Check if user is VIP"""
        user = self.get_user(user_id)
        return user.get("is_vip", False)
    
    def set_vip(self, user_id: int, is_vip: bool = True):
        """Set user's VIP status"""
        self.update_user(user_id, is_vip=is_vip)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get overall statistics"""
        total_users = len(self.users)
        total_searches = sum(user.get("searches", 0) for user in self.users.values())
        vip_users = sum(1 for user in self.users.values() if user.get("is_vip", False))
        
        return {
            "total_users": total_users,
            "vip_users": vip_users,
            "total_searches": total_searches
        }


# Global user manager instance
user_manager = UserManager()
