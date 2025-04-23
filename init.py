import yaml
import os
from databricks.sdk import WorkspaceClient
import subprocess
import sys
from typing import Dict, Any, List

def setup_databricks_authentication(workspace_url: str) -> None:
    """
    Sets up Databricks authentication using OAuth.
    
    Args:
        workspace_url: URL of the Databricks workspace
    """
    try:
        # Run databricks auth login command
        process = subprocess.Popen(
            ["databricks", "auth", "login", "--host", workspace_url],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate()
        
        if process.returncode != 0:
            print(f"Error during authentication: {stderr.decode()}")
            sys.exit(1)
            
        print("Authentication successful!")
    except FileNotFoundError:
        print("Error: databricks-cli not found. Please install it using: pip install databricks-cli")
        sys.exit(1)
    except Exception as e:
        print(f"Error during authentication: {str(e)}")
        sys.exit(1)

def get_warehouse_config(client: WorkspaceClient) -> Dict[str, str]:
    """
    Lists available SQL warehouses and lets the user select one.
    
    Args:
        client: Authenticated WorkspaceClient instance
    
    Returns:
        Dictionary containing warehouse_id and warehouse_name
    """
    try:
        # List all warehouses
        warehouses = list(client.warehouses.list())
        
        if not warehouses:
            print("No SQL warehouses found in your workspace.")
            sys.exit(1)
            
        print("\nAvailable SQL Warehouses:")
        for i, warehouse in enumerate(warehouses, 1):
            print(f"{i}. {warehouse.name} (ID: {warehouse.id})")
            
        while True:
            try:
                choice = int(input("\nSelect a warehouse (enter number): "))
                if 1 <= choice <= len(warehouses):
                    selected = warehouses[choice - 1]
                    return {
                        "warehouse_id": selected.id,
                        "warehouse_name": selected.name
                    }
                else:
                    print("Invalid selection. Please try again.")
            except ValueError:
                print("Please enter a valid number.")
    except Exception as e:
        print(f"Error getting warehouse configuration: {str(e)}")
        sys.exit(1)

def save_config(config: Dict[str, Any]) -> None:
    """
    Saves configuration to config.yaml
    
    Args:
        config: Configuration dictionary to save
    """
    try:
        with open("config.yaml", "w") as f:
            yaml.dump(config, f, default_flow_style=False)
        print("\nConfiguration saved to config.yaml")
    except Exception as e:
        print(f"Error saving configuration: {str(e)}")
        sys.exit(1)

def main():
    print("MCP Server Configuration Setup")
    print("=============================")
    
    # Get workspace URL
    workspace_url = input("\nEnter your Databricks workspace URL: ").strip()
    if not workspace_url:
        print("Workspace URL is required.")
        sys.exit(1)
        
    # Setup authentication
    setup_databricks_authentication(workspace_url)
    
    # Create client
    client = WorkspaceClient(host=workspace_url)
    
    # Get warehouse configuration
    warehouse_config = get_warehouse_config(client)
    
    # Get default catalog
    catalog = input("\nEnter default catalog (press Enter to skip): ").strip()
    
    # Create final config
    config = {
        "workspace": {
            "url": workspace_url,
            "warehouse_id": warehouse_config["warehouse_id"],
            "warehouse_name": warehouse_config["warehouse_name"],
            "catalog": catalog if catalog else ""
        }
    }
    
    # Save configuration
    save_config(config)
    
    print("\nConfiguration complete! You can now start the MCP server.")

if __name__ == "__main__":
    main() 