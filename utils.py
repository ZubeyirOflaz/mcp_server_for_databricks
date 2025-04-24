import yaml
import logging
from logging.handlers import RotatingFileHandler
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import ExecuteStatementRequest
from typing import Dict, Any, List, Optional
import os
from datetime import datetime

logger = logging.getLogger(__name__)

def validate_config_structure(config: Dict[str, Any]) -> bool:
    """
    Validates that the config file has the correct structure.
    
    Args:
        config: Configuration dictionary to validate
    
    Returns:
        bool: True if structure is valid, False otherwise
    """
    required_fields = {
        "workspace": {
            "url": str,
            "warehouse_id": str,
            "warehouse_name": str,
            "catalog": str
        }
    }
    
    try:
        logger.info("Validating config structure...")
        # Check if workspace section exists
        if "workspace" not in config:
            logger.error("Missing 'workspace' section in config")
            return False
            
        workspace = config["workspace"]
        
        # Check all required fields exist and are of correct type
        for field, field_type in required_fields["workspace"].items():
            if field not in workspace:
                logger.error(f"Missing required field '{field}' in workspace config")
                return False
            if not isinstance(workspace[field], field_type):
                logger.error(f"Field '{field}' has incorrect type. Expected {field_type}, got {type(workspace[field])}")
                return False
                
        logger.info("Config structure validation successful")
        return True
    except Exception as e:
        logger.error(f"Error validating config structure: {str(e)}")
        return False

def setup_logging(log_dir: str = "logs") -> None:
    """
    Sets up logging with monthly rotation.
    Creates a new log file for each month if it doesn't exist.
    
    Args:
        log_dir: Directory to store log files
    """
    try:
        if not os.path.exists(log_dir):
            logger.info(f"Creating log directory: {log_dir}")
            os.makedirs(log_dir)
        
        # Get current month and year for the log file name
        current_date = datetime.now()
        log_file = os.path.join(
            log_dir,
            f"mcp_server_{current_date.strftime('%Y_%m')}.log"
        )
        
        # Create a rotating file handler with a large maxBytes to prevent rotation
        # We'll handle monthly rotation through file naming instead
        handler = RotatingFileHandler(
            log_file,
            maxBytes=100*1024*1024,  # 100MB
            backupCount=0  # No backup files needed as we use monthly files
        )
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
        
        # Also log to console
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        logger.info("Logging setup completed successfully")
    except Exception as e:
        print(f"Error setting up logging: {str(e)}")
        raise

def load_config() -> Dict[str, Any]:
    """
    Loads and validates configuration from config.yaml
    
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
            
        if not validate_config_structure(config):
            logger.error("Invalid configuration structure")
            raise Exception(
                "Invalid configuration structure. Please run init.py to reconfigure."
            )
            
        logger.info("Configuration loaded successfully")
        return config
    except Exception as e:
        logger.error(f"Error loading config.yaml: {str(e)}")
        raise Exception(f"Error loading config.yaml: {str(e)}")

def get_table_metadata(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: Optional[str] = None,
    schema: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Gets metadata for all tables in the specified catalog and schema.
    
    Args:
        client: Authenticated WorkspaceClient instance
        warehouse_id: ID of the SQL warehouse to use
        catalog: Catalog name (optional)
        schema: Schema name (optional)
    
    Returns:
        List of table metadata dictionaries
    """
    try:
        # Build the query based on provided parameters
        query = "SHOW TABLES"
        if catalog:
            query = f"SHOW TABLES IN {catalog}"
            if schema:
                query = f"SHOW TABLES IN {catalog}.{schema}"
        
        logger.info(f"Executing query: {query}")
        # Execute the query
        result = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=query
        ).result()
        
        # Process the results
        tables = []
        for row in result.data_array:
            tables.append({
                "catalog": row[0],
                "schema": row[1],
                "name": row[2],
                "is_temporary": row[3] == "true"
            })
        logger.info(f"Retrieved metadata for {len(tables)} tables")
        return tables
    except Exception as e:
        logger.error(f"Error getting table metadata: {str(e)}")
        raise

def get_table_sample(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
    table: str,
    limit: int = 5
) -> List[Dict[str, Any]]:
    """
    Gets a sample of data from a specific table.
    
    Args:
        client: Authenticated WorkspaceClient instance
        warehouse_id: ID of the SQL warehouse to use
        catalog: Catalog name
        schema: Schema name
        table: Table name
        limit: Number of rows to return
    
    Returns:
        List of dictionaries containing the sample data
    """
    try:
        # Build and execute the query
        query = f"SELECT * FROM {catalog}.{schema}.{table} LIMIT {limit}"
        logger.info(f"Executing query: {query}")
        
        result = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=query
        ).result()
        
        # Process the results
        samples = []
        if result.data_array:
            # Get column names from the first row
            columns = [col.name for col in result.schema.columns]
            logger.info(f"Retrieved columns: {columns}")
            
            # Process each row
            for row in result.data_array:
                sample = {}
                for i, value in enumerate(row):
                    sample[columns[i]] = value
                samples.append(sample)
                
        logger.info(f"Retrieved {len(samples)} sample rows")
        return samples
    except Exception as e:
        logger.error(f"Error getting table sample: {str(e)}")
        raise 