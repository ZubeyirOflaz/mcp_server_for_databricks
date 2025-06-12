"""Main entry point for MCP Databricks server."""

import asyncio
from mcp_server_for_databricks.app import MCPDatabricksApp

async def main():
    """Main entry point for the application."""
    app = MCPDatabricksApp()
    await app.initialize()
    return app.get_mcp_server()

def run_server():
    """Run the MCP server."""
    # Create the app
    app = MCPDatabricksApp()
    
    # Initialize the app synchronously (letting FastMCP handle the event loop)
    async def init_app():
        await app.initialize()
        return app
    
    # Initialize first
    initialized_app = asyncio.run(init_app())
    
    # Then run the server (FastMCP will create its own event loop)
    initialized_app.run(transport='stdio')

if __name__ == "__main__":
    run_server() 