from databricks.sdk import WorkspaceClient
import subprocess
import sys
import webbrowser
import time
from urllib.parse import urlparse, parse_qs

def setup_databricks_authentication(workspace_url: str) -> None:
    """
    Automates the OAuth authentication process using the Databricks CLI.
    This only needs to be done once as it will cache the token locally.
    
    Args:
        workspace_url: The URL of your Databricks workspace
        (e.g., https://dbc-a1b2345c-d6e7.cloud.databricks.com)
    """
    try:
        # Run the databricks auth login command
        print(f"Starting Databricks authentication for workspace: {workspace_url}")
        process = subprocess.Popen(
            ["databricks", "auth", "login", "--host", workspace_url],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Wait for the process to complete
        stdout, stderr = process.communicate()
        
        if process.returncode != 0:
            print(f"Error during authentication: {stderr}")
            sys.exit(1)
            
        print("Authentication completed successfully!")
        
    except FileNotFoundError:
        print("Error: Databricks CLI not found. Please install it first using:")
        print("pip install databricks-cli")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error during authentication: {str(e)}")
        sys.exit(1)

def get_databricks_client(workspace_url: str) -> WorkspaceClient:
    """
    Creates a Databricks WorkspaceClient instance using the cached OAuth credentials.
    
    Args:
        workspace_url: The URL of your Databricks workspace
    
    Returns:
        WorkspaceClient: Authenticated Databricks client
    """
    return WorkspaceClient(host=workspace_url)

def example_api_calls(client: WorkspaceClient):
    """
    Example function demonstrating various API calls using the authenticated client.
    
    Args:
        client: Authenticated WorkspaceClient instance
    """
    try:
        # List all clusters
        print("\nListing clusters:")
        clusters = client.clusters.list()
        for cluster in clusters:
            print(f"Cluster: {cluster.cluster_name} (ID: {cluster.cluster_id})")
        
        # List all jobs
        print("\nListing jobs:")
        jobs = client.jobs.list()
        for job in jobs:
            print(f"Job: {job.settings.name} (ID: {job.job_id})")
            
    except Exception as e:
        print(f"Error making API calls: {str(e)}")

def main():
    # Replace this with your Databricks workspace URL
    workspace_url = "https://adb-8240274493362621.1.azuredatabricks.net/"
    
    # Setup authentication if needed
    setup_databricks_authentication(workspace_url)
    
    # Get authenticated client
    client = get_databricks_client()
    
    # Make example API calls
    example_api_calls(client)

if __name__ == "__main__":
    main()