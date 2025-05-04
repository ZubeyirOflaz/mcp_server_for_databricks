import httpx
import sys
import logging
import subprocess
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from databricks.sdk import WorkspaceClient
from typing import List, Dict, Any, Optional
from mcp.server.fastmcp import FastMCP
from logging.handlers import RotatingFileHandler
from utils import (
    load_config,
    get_table_metadata,
    get_table_sample
)
import asyncio
import os
from datetime import datetime
import json

# Define data models
class TableMetadata(BaseModel):
    catalog: str
    schema_name: str
    name: str
    is_temporary: bool

class TableSample(BaseModel):
    data: List[Dict[str, Any]]

class SchemaInfo(BaseModel):
    catalog: str
    schema_name: str
    tables: List[str]

# Constants
login_initialization_complete = False
config = None
workspace_config = None
client = None
logger = None

def setup_logging(log_dir: str = ".logs", log_filename: str = "mcp_unity.log") -> logging.Logger:
    """
    Set up logging configuration to write to a file and console.
    Creates the log directory if it doesn't exist.
    
    Args:
        log_dir: Directory to store log files
        log_filename: Name of the log file
        
    Returns:
        Configured root logger
    """
    # Create log directory if it doesn't exist
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_file = os.path.join(log_dir, log_filename)
    
    # First clear any existing handlers to avoid duplicates
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create file handler with immediate flush
    file_handler = RotatingFileHandler(
        log_file, 
        maxBytes=10*1024*1024, 
        backupCount=5,
        delay=False  # Open the file immediately
    )
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    
    # Configure the root logger
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # Log the setup and flush immediately
    root_logger.info(f"Logging configured to write to {log_file}")
    for handler in root_logger.handlers:
        handler.flush()
    
    return root_logger

def get_logger(name: str) -> logging.Logger:
    """
    Get a logger configured to write to the .logs directory.
    This function ensures all loggers use the same configuration.
    
    Args:
        name: Name for the logger (typically __name__ from the calling module)
        
    Returns:
        Properly configured logger
    """
    # Use the root logger's handlers
    return logging.getLogger(name)

async def databricks_login(host: str) -> bool:
    """
    Perform Databricks login using the CLI with the mcp_server_for_databricks profile.
    
    Args:
        host: Databricks workspace URL
        
    Returns:
        bool: True if login was successful, False otherwise
    """
    login_success = False
    profile_name = "mcp_server_for_databricks"
    
    try:
        # Check if already authenticated
        logging.info(f"Checking Databricks authentication status for host: {host} using profile: {profile_name}")
        
        # Use Popen instead of run to be able to capture output even on timeout
        process = subprocess.Popen(
            ["databricks", "auth", "token", "--host", host, "--profile", profile_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        try:
            stdout, stderr = process.communicate(timeout=15)
            returncode = process.returncode
            
            if returncode == 0:
                logging.info(f"Already authenticated with Databricks using profile: {profile_name}")
                return True
                
        except subprocess.TimeoutExpired:
            # Try to get output even after timeout
            logging.error("Timeout during Databricks auth check")
            try:
                process.kill()
                stdout, stderr = process.communicate()
                logging.error(f"Auth check timeout - stdout before timeout: {stdout}")
                logging.error(f"Auth check timeout - stderr before timeout: {stderr}")
            except Exception as kill_e:
                logging.error(f"Error killing process after timeout: {str(kill_e)}")
    
    except Exception as e:
        logging.error(f"Error during Databricks auth check: {str(e)} \n Now trying to execute initial login")
    
    # If we reach here, either auth check failed, timed out, or errored - try login
    logging.info(f"Starting Databricks authentication login flow with profile: {profile_name}...")
    try:
        process = subprocess.Popen(
            ["databricks", "auth", "login", "--host", host, "--profile", profile_name],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Wait for the process to complete with a timeout
        try:
            stdout, stderr = process.communicate(timeout=30)
            logging.info(f"Login process stdout: {stdout}")
            logging.info(f"Login process stderr: {stderr}")
            
            if process.returncode != 0:
                logging.error(f"Databricks login failed with return code: {process.returncode}")
                logging.error(f"Databricks login stderr: {stderr}")
                logging.error(f"Databricks login stdout: {stdout}")
                return False
            logging.info(f"Successfully logged in to Databricks with profile: {profile_name}")
            return True
        except subprocess.TimeoutExpired:
            # Try to get output even after timeout
            try:
                process.kill()
                stdout, stderr = process.communicate()
                logging.error(f"Login timeout - stdout before timeout: {stdout}")
                logging.error(f"Login timeout - stderr before timeout: {stderr}")
            except Exception as kill_e:
                logging.error(f"Error killing process after timeout: {str(kill_e)}")
            logging.error("Databricks login timed out")
            return False
    except Exception as login_e:
        logging.error(f"Exception during login process: {str(login_e)}")
        return False
    return login_success
            
    

async def initialize_globals():
    global login_initialization_complete
    global config
    global workspace_config
    global client
    global logger
    if login_initialization_complete:
        return None
    
    # Set up logging
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("Initializing globals...")
    
    try:
        config = await load_config(logger)
        workspace_config = config["workspace"]
        logger.info(f"Loaded config: {config}")
        
        # Validate Databricks configuration
        if "url" not in config["workspace"]:
            raise ValueError("Missing url in workspace config")
            
        databricks_host = config["workspace"]["url"]
        logger.info(f"Using Databricks host: {databricks_host}")
       
        # Initialize Databricks client first
        try:
            # First try to authenticate
            logger.info("Attempting Databricks authentication...")
            auth_result = await databricks_login(databricks_host)
            logger.info(f"Authentication result: {auth_result}")
            
            if not auth_result:
                raise ValueError("Failed to authenticate with Databricks. Please check your credentials.")
            
            # Initialize client with default profile
            logger.info("Creating Databricks WorkspaceClient...")
            
            # Get the token from CLI and parse the JSON response
            profile_name = "mcp_server_for_databricks"
            token_output = subprocess.check_output(["databricks", "auth", "token", "--host", databricks_host, "--profile", profile_name]).decode("utf-8").strip()
            logger.info(f"Successfully retrieved auth token using profile: {profile_name}")
            
            try:
                # Parse the JSON output
                token_data = json.loads(token_output)
                access_token = token_data.get("access_token")
                
                if not access_token:
                    logger.error("Failed to extract access_token from token response")
                    logger.error(f"Token response: {token_output}")
                    raise ValueError("Could not extract access_token from Databricks token response")
                
                logger.info("Successfully extracted access_token")
                
                # Create client with the extracted token
                client = WorkspaceClient(
                    host=databricks_host,
                    token=access_token
                )
                logger.info("WorkspaceClient created successfully with access_token")
            except json.JSONDecodeError:
                logger.error(f"Failed to parse token output as JSON: {token_output}")
                raise ValueError("Invalid JSON response from databricks auth token command")
            
            # Test the connection
            logger.info("Testing Databricks connection by listing catalogs...")
            catalogs = client.catalogs.list()
            catalog_names = [c.name for c in catalogs]
            logger.info(f"Successfully connected to Databricks workspace. Available catalogs: {catalog_names}")
                
        except Exception as e:
            logger.error(f"Failed to connect to Databricks: {type(e).__name__}: {str(e)}")
            if hasattr(e, '__traceback__'):
                import traceback
                tb_str = ''.join(traceback.format_tb(e.__traceback__))
                logger.error(f"Traceback: {tb_str}")
            raise ValueError(
                f"Failed to connect to Databricks workspace: {str(e)}. "
                "Please check your authentication and permissions."
            )
            
        login_initialization_complete = True
        logger.info("Global initialization completed successfully")
    except Exception as e:
        logger.error(f"Error during initialization: {type(e).__name__}: {str(e)}")
        if hasattr(e, '__traceback__'):
            import traceback
            tb_str = ''.join(traceback.format_tb(e.__traceback__))
            logger.error(f"Traceback: {tb_str}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to initialize application: {str(e)}"
        )
    
# Initialize FastMCP server
mcp = FastMCP("mcp_unity")


@mcp.tool()
async def get_schemas(catalog: str) -> List[SchemaInfo]:
    """
    Get all schemas and their tables in the workspace for the default catalog.
    """
    global login_initialization_complete, client, workspace_config, logger
    
    try:
        if not login_initialization_complete:
            await initialize_globals()
            
        logger.info("Globals initialized: %s", login_initialization_complete)
        logger.info("Getting schemas...")
        tables = await get_table_metadata(
            client,
            workspace_config["warehouse_id"],
            catalog=catalog,
            logger=logger
        )
        
        # Group tables by schema
        schemas: Dict[str, List[str]] = {}
        for table in tables:
            schema_key = f"{table['catalog']}.{table['schema']}"
            if schema_key not in schemas:
                schemas[schema_key] = []
            schemas[schema_key].append(table["name"])
        
        # Convert to SchemaInfo objects
        result = []
        for schema_key, table_names in schemas.items():
            catalog, schema_name = schema_key.split(".")
            result.append(SchemaInfo(
                catalog=catalog,
                schema_name=schema_name,
                tables=table_names
            ))
        
        logger.info(f"Found {len(result)} schemas")
        return result
    except Exception as e:
        logger.error(f"Error getting schemas: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@mcp.tool()
async def get_table_sample_tool(catalog: str, schema_name: str, table: str) -> Dict[str, Any]:
    """
    Return and save detailed table metadata, including integrated sample data, for a given table.
    This function will save the sample data and table metadata to the .input_data folder if configured.
    Args:
        catalog: Catalog name
        schema_name: Schema name
        table: Table name
    Returns:
        Dictionary with detailed table metadata including sample values.
    """
    global login_initialization_complete, client, workspace_config, logger
    try:
        if not login_initialization_complete:
            await initialize_globals()
            
        logger.info(f"Getting table metadata and sample data for {catalog}.{schema_name}.{table}")

        # Call get_table_sample which now returns only metadata with integrated sample values
        table_metadata = await get_table_sample(
            client=client, # Use the global client
            warehouse_id=workspace_config["warehouse_id"],
            catalog=catalog,
            schema=schema_name,
            table=table,
            logger=logger
        )

        # Return the comprehensive metadata dictionary
        return table_metadata
    except Exception as e:
        logger.error(f"Error getting table sample: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@mcp.tool()
async def get_schema_metadata(catalog_name:str, schema_name:str):
    """
    This function will return the schema metadata for a given schema.
    Args:
        catalog_name: Catalog name
        schema_name: Schema name
    Returns:
        Dictionary with schema metadata, schema metadata is provided in the following format:
        {
            "schema_comment": "Schema comment",
            "tables": {
                "table_name": {
                    "comment": "Table comment",
                    "created_at": "Table created at",
                    "table_type": "Table type",
                    "owner": "Table owner"
                }
            }
        }
    """
  
    global login_initialization_complete, client, workspace_config, logger
    
    try:
        if not login_initialization_complete:
            #return "Server initialization is not completed, please wait for the server to complete startup and try again."
            await initialize_globals()
        # Get the comment for the schema if it exists
        schema_comment = client.schemas.get(f"{catalog_name}.{schema_name}").comment
        tables = client.tables.list(catalog_name, schema_name)
        schema_metadata = {}
        schema_metadata['schema_comment'] = schema_comment
        schema_metadata['tables'] = {}
        for table in tables:
            schema_metadata['tables'][table.name] = {
                'comment' : table.comment,
                'created_at': table.created_at,
                'table_type': table.table_type,
                'owner': table.owner}
        return schema_metadata
    except Exception as e:
        logger.error(f"Error getting table sample: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    

if __name__ == "__main__":
    # Run everything in a single event loop
    mcp.run(transport='stdio')

