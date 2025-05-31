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
    # Create and run the server
    app = MCPDatabricksApp()
    
    # Initialize and run in the same event loop
    async def init_and_run():
        await app.initialize()
        app.run(transport='stdio')
    
    # Run everything in a single event loop
    asyncio.run(init_and_run())

if __name__ == "__main__":
    run_server() 