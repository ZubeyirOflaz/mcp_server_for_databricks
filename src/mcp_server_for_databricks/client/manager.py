"""Databricks WorkspaceClient lifecycle management."""

import logging
from typing import Optional, Dict, Any
from databricks.sdk import WorkspaceClient

from mcp_server_for_databricks.auth.databricks_auth import databricks_login
from mcp_server_for_databricks.auth.token_manager import TokenManager

class ClientManager:
    """Manages the Databricks WorkspaceClient and its authentication."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.client: Optional[WorkspaceClient] = None
        self.token_manager = TokenManager()
        self.logger = logging.getLogger(__name__)
        self._initialization_complete = False
    
    async def initialize(self) -> None:
        """Initialize the client and authentication."""
        if self._initialization_complete and not self.token_manager.is_token_expired():
            self.logger.info("Client already initialized and token is valid")
            return
        
        if self.token_manager.is_token_expired():
            self.logger.info("Token is expired, refreshing authentication")
            await self._refresh_authentication()
        else:
            await self._full_initialization()
    
    async def _full_initialization(self) -> None:
        """Perform full initialization including authentication."""
        self.logger.info("Performing full client initialization...")
        
        # Validate configuration
        if not self.config.get("workspace", {}).get("url"):
            raise ValueError("Missing workspace URL in configuration")
        
        databricks_host = self.config["workspace"]["url"]
        self.logger.info(f"Initializing with Databricks host: {databricks_host}")
        
        # Authenticate with Databricks
        auth_result = await databricks_login(databricks_host)
        if not auth_result:
            raise ValueError("Failed to authenticate with Databricks. Please check your credentials.")
        
        # Get token and create client
        access_token = self.token_manager.get_valid_token(databricks_host)
        self.client = WorkspaceClient(
            host=databricks_host,
            token=access_token
        )
        
        self.logger.info("WorkspaceClient created successfully")
        self._initialization_complete = True
    
    async def _refresh_authentication(self) -> None:
        """Refresh authentication and recreate client."""
        self.logger.info("Refreshing authentication and client...")
        
        databricks_host = self.config["workspace"]["url"]
        access_token = self.token_manager.get_valid_token(databricks_host)
        
        self.client = WorkspaceClient(
            host=databricks_host,
            token=access_token
        )
        
        self.logger.info("Client refreshed successfully")
    
    def get_client(self) -> WorkspaceClient:
        """
        Get the WorkspaceClient instance.
        
        Returns:
            WorkspaceClient: The initialized client
            
        Raises:
            RuntimeError: If client is not initialized
        """
        if self.client is None:
            raise RuntimeError("Client not initialized. Call initialize() first.")
        return self.client
    
    def is_initialized(self) -> bool:
        """Check if the client is initialized."""
        return self._initialization_complete and self.client is not None 