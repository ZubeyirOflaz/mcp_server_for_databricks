"""Token management for Databricks authentication."""

import logging
from datetime import datetime, timezone
from typing import Optional

from mcp_server_for_databricks.auth.databricks_auth import get_databricks_token

class TokenManager:
    """Manages Databricks authentication tokens and their expiry."""
    
    def __init__(self):
        self.access_token: Optional[str] = None
        self.token_expiry_datetime: Optional[datetime] = None
        self.logger = logging.getLogger(__name__)
    
    def is_token_expired(self) -> bool:
        """
        Check if the current token is expired.
        
        Returns:
            bool: True if token is expired or not set, False otherwise
        """
        if self.token_expiry_datetime is None:
            return True
        return datetime.now(timezone.utc) > self.token_expiry_datetime
    
    def refresh_token(self, host: str) -> str:
        """
        Refresh the authentication token.
        
        Args:
            host: Databricks workspace URL
            
        Returns:
            str: New access token
            
        Raises:
            ValueError: If token refresh fails
        """
        try:
            self.logger.info("Refreshing Databricks authentication token")
            self.access_token, self.token_expiry_datetime = get_databricks_token(host)
            self.logger.info("Token refreshed successfully")
            return self.access_token
        except Exception as e:
            self.logger.error(f"Failed to refresh token: {e}")
            raise ValueError(f"Token refresh failed: {e}")
    
    def get_valid_token(self, host: str) -> str:
        """
        Get a valid authentication token, refreshing if necessary.
        
        Args:
            host: Databricks workspace URL
            
        Returns:
            str: Valid access token
            
        Raises:
            ValueError: If token cannot be obtained
        """
        if self.is_token_expired():
            return self.refresh_token(host)
        return self.access_token 