#!/usr/bin/env python3
"""Test script to validate the refactored MCP server works correctly."""

import asyncio
import sys
import os

# Add src to path to import the modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from mcp_server_for_databricks.app import MCPDatabricksApp

async def test_server():
    """Test server initialization."""
    print("🧪 Testing MCP Databricks server initialization...")
    
    try:
        # Create app
        app = MCPDatabricksApp()
        print("✅ App created successfully")
        
        # Initialize app
        await app.initialize()
        print("✅ App initialized successfully")
        
        # Get MCP server
        mcp_server = app.get_mcp_server()
        print("✅ MCP server retrieved successfully")
        
        # Check tools are registered
        tools_response = await mcp_server.list_tools()
        tools = tools_response.tools if hasattr(tools_response, 'tools') else []
        print(f"✅ Found {len(tools)} tools registered:")
        for tool in tools:
            print(f"   - {tool.name}: {tool.description}")
        
        print("\n🎉 All tests passed! The refactored server is working correctly.")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = asyncio.run(test_server())
    sys.exit(0 if success else 1) 