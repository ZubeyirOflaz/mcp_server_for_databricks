#!/usr/bin/env python3
"""Detailed test script to validate the refactored MCP server and tool registration."""

import asyncio
import sys
import os

# Add src to path to import the modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from mcp_server_for_databricks.app import MCPDatabricksApp

async def test_server_detailed():
    """Test server initialization and tool registration in detail."""
    print("🔍 Detailed MCP Databricks server test...")
    
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
        print(f"   Server type: {type(mcp_server)}")
        
        # Check internal tool registry
        print(f"   FastMCP tools registry: {len(mcp_server._tools) if hasattr(mcp_server, '_tools') else 'Unknown'}")
        
        # Check for tools in different ways
        try:
            # Method 1: list_tools()
            print("\n🔧 Checking tools via list_tools()...")
            tools_response = await mcp_server.list_tools()
            print(f"   Response type: {type(tools_response)}")
            print(f"   Response: {tools_response}")
            
            if hasattr(tools_response, 'tools'):
                tools = tools_response.tools
                print(f"   Found {len(tools)} tools via list_tools()")
                for tool in tools:
                    print(f"     - {tool.name}: {tool.description}")
            else:
                print("   No 'tools' attribute found in response")
                
        except Exception as e:
            print(f"   ❌ Error with list_tools(): {e}")
        
        # Method 2: Check internal _tools attribute
        try:
            print("\n🔧 Checking internal _tools attribute...")
            if hasattr(mcp_server, '_tools'):
                internal_tools = mcp_server._tools
                print(f"   Found {len(internal_tools)} internal tools:")
                for name, tool in internal_tools.items():
                    print(f"     - {name}: {type(tool)}")
            else:
                print("   No _tools attribute found")
        except Exception as e:
            print(f"   ❌ Error checking internal tools: {e}")
            
        # Method 3: Check if server has specific methods
        print("\n🔧 Checking server attributes...")
        attrs = [attr for attr in dir(mcp_server) if not attr.startswith('_')]
        print(f"   Public attributes: {attrs[:10]}...")  # Show first 10
        
        print("\n🎉 Detailed test completed!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = asyncio.run(test_server_detailed())
    sys.exit(0 if success else 1) 