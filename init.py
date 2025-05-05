import yaml
import os
from databricks.sdk import WorkspaceClient
import subprocess
import sys
import json
from typing import Dict, Any, List

def setup_databricks_authentication(workspace_url: str) -> str:
    """
    Sets up Databricks authentication using OAuth with the profile name "mcp_server_for_databricks"
    and returns the access token for SDK usage.
    
    Args:
        workspace_url: URL of the Databricks workspace
        
    Returns:
        The Databricks access token
    """
    profile_name = "mcp_server_for_databricks"
    
    try:
        # Run databricks auth login command with profile name
        process = subprocess.Popen(
            ["databricks", "auth", "login", "--host", workspace_url, "--profile", profile_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate()
        
        if process.returncode != 0:
            print(f"Error during authentication: {stderr.decode()}")
            sys.exit(1)
            
        print(f"Authentication successful with profile '{profile_name}'")
        
        # Get the token from CLI using the same profile
        try:
            token_output = subprocess.check_output(
                ["databricks", "auth", "token", "--host", workspace_url, "--profile", profile_name]
            ).decode("utf-8").strip()
            
            # Parse the JSON output
            token_data = json.loads(token_output)
            access_token = token_data.get("access_token")
            
            if not access_token:
                print("Failed to extract access_token from token response")
                sys.exit(1)
                
            return access_token
            
        except json.JSONDecodeError:
            print(f"Failed to parse token output as JSON")
            sys.exit(1)
            
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
    access_token = setup_databricks_authentication(workspace_url)
    
    # Create client
    client = WorkspaceClient(
        host=workspace_url,
        token=access_token
    )
    
    # Get warehouse configuration
    warehouse_config = get_warehouse_config(client)
    
    # Get sample size
    while True:
        try:
            sample_size_input = input("\nEnter the desired sample size (number of rows) for retrieving table data [default: 5]: ").strip()
            if not sample_size_input:
                sample_size = 5  # Default value
                print("Using default sample size of 5.")
                break
            sample_size = int(sample_size_input)
            if sample_size > 0:
                break
            else:
                print("Sample size must be a positive integer.")
        except ValueError:
            print("Invalid input. Please enter an integer.")
            
    
    # Create final config
    config = {
        "workspace": {
            "url": workspace_url,
            "warehouse_id": warehouse_config["warehouse_id"],
            "warehouse_name": warehouse_config["warehouse_name"],
            "sample_size": sample_size
        }
    }
    
    # Save configuration
    save_config(config)
    
    print("\nConfiguration complete! You can now start the MCP server.")

if __name__ == "__main__":
    main() 