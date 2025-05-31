"""Configuration loading and validation."""

import yaml
import logging
import os
from typing import Dict, Any

from mcp_server_for_databricks.config.models import AppConfig, WorkspaceConfig

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
            "sample_size": int,
            "warehouse_name": str
        }
    }
    
    optional_fields = {
        "workspace": {
            "catalog": str,
            "profile": str,
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
            config_dict = yaml.safe_load(f)
            
        if not await validate_config_structure(config_dict, logger):
            logger.error("Invalid configuration structure")
            raise Exception(
                "Invalid configuration structure. Please run init.py to reconfigure."
            )
        
        logger.info("Configuration loaded successfully")
        return config_dict
        
    except Exception as e:
        logger.error(f"Error loading config.yaml: {str(e)}")
        raise Exception(f"Error loading config.yaml: {str(e)}") 