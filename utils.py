import yaml
import logging
from logging.handlers import RotatingFileHandler
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import ExecuteStatementRequest, Disposition, Format, ExecuteStatementRequestOnWaitTimeout, StatementState
from typing import Dict, Any, List, Optional
import os
from datetime import datetime
import asyncio
import csv
import json
async def validate_config_structure(config: Dict[str, Any], logger: logging.Logger) -> bool:
    """
    Validates that the config file has the correct structure.
    
    Args:
        config: Configuration dictionary to validate
        logger: Logger instance to use
    
    Returns:
        bool: True if structure is valid, False otherwise
    """
    required_fields = {
        "workspace": {
            "url": str,
            "warehouse_id": str,
            "warehouse_name": str
        }
    }
    
    optional_fields = {
        "workspace": {
            "catalog": str,
            "profile": str,
            "sample_size": int,
            "wait_timeout": str,
            "save_table_metadata": bool
        }
    }
    
    try:
        logger.info("Validating config structure...")
        if "workspace" not in config:
            logger.error("Missing 'workspace' section in config")
            return False
            
        workspace = config["workspace"]
        
        # Check required fields
        for field, field_type in required_fields["workspace"].items():
            if field not in workspace:
                logger.error(f"Missing required field '{field}' in workspace config")
                return False
            if not isinstance(workspace[field], field_type):
                logger.error(f"Field '{field}' has incorrect type. Expected {field_type}, got {type(workspace[field])}")
                return False
        
        # Check optional fields if present
        for field, field_type in optional_fields["workspace"].items():
            if field in workspace and not isinstance(workspace[field], field_type):
                logger.error(f"Field '{field}' has incorrect type. Expected {field_type}, got {type(workspace[field])}")
                return False
                
        logger.info("Config structure validation successful")
        return True
    except Exception as e:
        logger.error(f"Error validating config structure: {str(e)}")
        return False

async def load_config(logger: logging.Logger) -> Dict[str, Any]:
    """
    Loads and validates configuration from config.yaml
    
    Args:
        logger: Logger instance to use
    
    Returns:
        Configuration dictionary
    
    Raises:
        Exception: If config file doesn't exist or has invalid structure
    """
    try:
        logger.info("Loading configuration from config.yaml...")
        if not os.path.exists("config.yaml"):
            logger.error("config.yaml file not found")
            raise Exception(
                "Configuration file not found. Please run init.py first to set up your configuration."
            )
            
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)
            
        if not await validate_config_structure(config, logger):
            logger.error("Invalid configuration structure")
            raise Exception(
                "Invalid configuration structure. Please run init.py to reconfigure."
            )
            
        logger.info("Configuration loaded successfully")
        return config
    except Exception as e:
        logger.error(f"Error loading config.yaml: {str(e)}")
        raise Exception(f"Error loading config.yaml: {str(e)}")

async def get_table_metadata(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    logger: Optional[logging.Logger] = None
) -> List[Dict[str, Any]]:
    """
    Gets metadata for all tables in the specified catalog and schema.
    
    Args:
        client: Authenticated WorkspaceClient instance
        warehouse_id: ID of the SQL warehouse to use
        catalog: Catalog name (optional)
        schema: Schema name (optional)
        logger: Logger instance to use (optional)
    
    Returns:
        List of table metadata dictionaries
    
    Raises:
        ValueError: If warehouse_id is invalid or connection fails
        Exception: For other unexpected errors
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    # Read the config file
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)
    

    try:
        # Validate warehouse_id
        if not warehouse_id:
            raise ValueError("Warehouse ID is required")
            
        # Use default value for wait_timeout if not in config
        wait_timeout = "30s"  # Default wait timeout
        
        # Override with config value if present
        if "workspace" in config and "wait_timeout" in config["workspace"]:
            wait_timeout = config["workspace"]["wait_timeout"]
            
        # Build the query based on provided parameters
        query = "SHOW DATABASES"
        if catalog:
            query = f"SHOW DATABASES IN {catalog}"
            if schema:
                query = f"SHOW TABLES IN {catalog}.{schema}"
        
        logger.info(f"Executing query: {query}")
        
        # Execute the query with error handling
        try:
            # Execute the statement with proper parameters
            response = client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=query,
                wait_timeout=wait_timeout,  # Wait up to 30 seconds
                on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,  # Continue asynchronously if timeout
                disposition=Disposition.INLINE,  # Get results inline
                format=Format.JSON_ARRAY  # Use JSON array format
            )
            
            # Get the statement ID
            statement_id = response.statement_id
            logger.info(f"Statement ID: {statement_id}")
            
            # Get the result using the statement ID
            result = client.statement_execution.get_statement(statement_id)
            
            # Check if the statement is still running
            while result.status.state in ["PENDING", "RUNNING"]:
                logger.info(f"Statement state: {result.status.state}")
                await asyncio.sleep(1)  # Wait for 1 second before checking again
                result = client.statement_execution.get_statement(statement_id)
            
            if result.status.state != StatementState.SUCCEEDED:
                error_message = f"Statement execution failed with state: {result.status.state}"
                if result.status.error:
                    error_message += f", Error: {result.status.error.message}"
                raise ValueError(error_message)
            
        except Exception as e:
            logger.error(f"Failed to execute query: {str(e)}")
            raise ValueError(f"Failed to execute query: {str(e)}")
        
        # Process the results
        tables = []
        if not result.result or not result.result.data_array:
            logger.warning("No tables found")
            return tables
            
        for row in result.result.data_array:
            try:
                tables.append({
                    "catalog": row[0],
                    "schema": row[1],
                    "name": row[2],
                    "is_temporary": row[3] == "true"
                })
            except IndexError as e:
                logger.error(f"Unexpected row format: {row}")
                continue
                
        logger.info(f"Retrieved metadata for {len(tables)} tables")
        return tables
    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error getting table metadata: {str(e)}")
        raise


async def get_table_sample(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
    table: str,
    logger: Optional[logging.Logger] = None
) -> List[Dict[str, Any]]:
    """
    Gets metadata for all tables in the specified catalog and schema.
    
    Args:
        client: Authenticated WorkspaceClient instance
        warehouse_id: ID of the SQL warehouse to use
        catalog: Catalog name (optional)
        schema: Schema name (optional)
        logger: Logger instance to use (optional)
    
    Returns:
        List of table metadata dictionaries
    
    Raises:
        ValueError: If warehouse_id is invalid or connection fails
        Exception: For other unexpected errors
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    # Read the config file
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)
    
    try:
        # Validate warehouse_id
        if not warehouse_id:
            raise ValueError("Warehouse ID is required")
            
        # Use default values if not in config
        sample_size = 100  # Default sample size
        wait_timeout = "30s"  # Default wait timeout
        
        # Override with config values if present
        if "workspace" in config:
            if "sample_size" in config["workspace"]:
                sample_size = config["workspace"]["sample_size"]
            if "wait_timeout" in config["workspace"]:
                wait_timeout = config["workspace"]["wait_timeout"]
            
        # Build the query based on provided parameters
        query = f"SELECT * FROM {catalog}.{schema}.{table} LIMIT {sample_size}"
        
        logger.info(f"Executing query: {query}")
        
        # Execute the query with error handling
        try:
            # Execute the statement with proper parameters
            response = client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=query,
                wait_timeout=wait_timeout,  
                on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,  # Continue asynchronously if timeout
                disposition=Disposition.INLINE,  # Get results inline
                format=Format.JSON_ARRAY  # Use JSON array format
            )
            
            # Get the statement ID
            statement_id = response.statement_id
            logger.info(f"Statement ID: {statement_id}")
            
            # Get the result using the statement ID
            result = client.statement_execution.get_statement(statement_id)
            
            # Check if the statement is still running
            while result.status.state in ["PENDING", "RUNNING"]:
                logger.info(f"Statement state: {result.status.state}")
                await asyncio.sleep(1)  # Wait for 1 second before checking again
                result = client.statement_execution.get_statement(statement_id)
            
            if result.status.state != StatementState.SUCCEEDED:
                error_message = f"Statement execution failed with state: {result.status.state}"
                if result.status.error:
                    error_message += f", Error: {result.status.error.message}"
                raise ValueError(error_message)
            
        except Exception as e:
            logger.error(f"Failed to execute query: {str(e)}")
            raise ValueError(f"Failed to execute query: {str(e)}")

        
        sample_data = result.result.as_dict()['data_array']
        table_schema = result.manifest.schema.as_dict()['columns']
        column_names = [col['name'] for col in table_schema]

        
        sample_dict = [dict(zip(column_names, row)) for row in sample_data]
        
        # Default to saving table metadata
        save_table_metadata = False
        
        # Check if save_table_metadata is specified in config
        if "workspace" in config and "save_table_metadata" in config["workspace"]:
            save_table_metadata = config["workspace"]["save_table_metadata"]
            
        if save_table_metadata:
            # Check if .input_data folder exists, if not create it
            if not os.path.exists("./.input_data"):
                os.makedirs("./.input_data")
                # Add .input_data to .gitignore
                if not os.path.exists("./.gitignore"):
                    with open("./.gitignore", "w") as f:
                        f.write(".input_data\n")
                else:
                    with open("./.gitignore", "a") as f:
                        f.write(".input_data\n")

            # Create a folder for the table
            table_folder = f"./.input_data/{catalog}/{schema}/{table}"
            if not os.path.exists(table_folder):
                os.makedirs(table_folder)
            with open(f"{table_folder}/sample_data.json", "w") as f:
                json.dump(sample_dict, f, indent=4)
            with open(f"{table_folder}/table_metadata.json", "w") as f:
                json.dump(table_schema, f, indent=4)
        
        
                
        logger.info(f"Retrieved sample data for {catalog}.{schema}.{table} table")
        return table_schema, sample_dict
    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error getting table metadata: {str(e)}")
        raise