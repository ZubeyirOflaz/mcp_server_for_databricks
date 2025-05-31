"""Databricks schema operations."""

import logging
import asyncio
from typing import Optional, List, Dict, Any
from databricks.sdk import WorkspaceClient

async def get_schema_list(
    client: WorkspaceClient,
    catalog: str,
    logger: Optional[logging.Logger] = None
) -> List[Dict[str, Any]]:
    """
    Gets metadata for all schemas in the specified catalog.
    
    Args:
        client: Authenticated WorkspaceClient instance
        catalog: Catalog name
        logger: Logger instance to use (optional)
    
    Returns:
        List of schema metadata dictionaries
    
    Raises:
        Exception: For unexpected errors
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    try:
        schema_list = await asyncio.to_thread(
            client.schemas.list,
            catalog_name=catalog
        )
        schema_list = [i.as_dict() for i in schema_list]
        return list(schema_list)
    except Exception as e:
        logger.error(f"Error getting schema list: {str(e)}")
        raise

async def get_schema_metadata(
    client: WorkspaceClient,
    catalog_name: str,
    schema_name: str,
    logger: Optional[logging.Logger] = None
) -> Dict[str, Any]:
    """
    Get detailed metadata for a specific schema including tables.
    
    Args:
        client: Authenticated WorkspaceClient instance
        catalog_name: Catalog name
        schema_name: Schema name
        logger: Logger instance to use (optional)
    
    Returns:
        Dictionary with schema metadata in the format:
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
    if logger is None:
        logger = logging.getLogger(__name__)
    
    try:
        # Get the comment for the schema if it exists
        schema_comment = client.schemas.get(f"{catalog_name}.{schema_name}").comment
        tables = client.tables.list(catalog_name, schema_name)
        
        schema_metadata = {
            'schema_comment': schema_comment,
            'tables': {}
        }
        
        for table in tables:
            schema_metadata['tables'][table.name] = {
                'comment': table.comment,
                'created_at': table.created_at,
                'table_type': table.table_type,
                'owner': table.owner
            }
        
        return schema_metadata
    except Exception as e:
        logger.error(f"Error getting schema metadata: {str(e)}")
        raise 