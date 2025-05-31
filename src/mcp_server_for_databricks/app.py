"""Main application setup and orchestration."""

import logging
from mcp.server.fastmcp import FastMCP

from mcp_server_for_databricks.utils.logging import setup_logging, get_logger
from mcp_server_for_databricks.config.loader import load_config
from mcp_server_for_databricks.client.manager import ClientManager
from mcp_server_for_databricks.mcp_tools.registry import create_mcp_server

class MCPDatabricksApp:
    """Main application class for MCP Databricks server."""
    
    def __init__(self):
        self.logger = None
        self.config = None
        self.client_manager = None
        self.mcp_server = None
    
    async def initialize(self) -> None:
        """Initialize the application."""
        # Setup logging first
        setup_logging()
        self.logger = get_logger(__name__)
        self.logger.info("Starting MCP Databricks server initialization...")
        
        try:
            # Load configuration
            self.config = await load_config(self.logger)
            self.logger.info("Configuration loaded successfully")
            
            # Initialize client manager
            self.client_manager = ClientManager(self.config)
            self.logger.info("Client manager created")
            
            # Create MCP server with all tools
            self.mcp_server = create_mcp_server(self.client_manager)
            self.logger.info("MCP server created with all tools")
            
            self.logger.info("Application initialization completed successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize application: {e}")
            raise
    
    def get_mcp_server(self) -> FastMCP:
        """Get the configured MCP server."""
        if self.mcp_server is None:
            raise RuntimeError("Application not initialized. Call initialize() first.")
        return self.mcp_server
    
    def run(self, transport: str = 'stdio') -> None:
        """Run the MCP server."""
        if self.mcp_server is None:
            raise RuntimeError("Application not initialized. Call initialize() first.")
        
        self.logger.info(f"Starting MCP server with transport: {transport}")
        self.mcp_server.run(transport=transport) 