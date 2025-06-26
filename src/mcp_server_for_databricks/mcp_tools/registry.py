"""MCP tool registry and server setup."""

import logging
from typing import Dict, Any
from fastapi import HTTPException
from mcp.server.fastmcp import FastMCP

from mcp_server_for_databricks.client.manager import ClientManager
from mcp_server_for_databricks.databricks.schemas import get_schema_list, get_schema_metadata
from mcp_server_for_databricks.databricks.tables import get_table_sample
from mcp_server_for_databricks.databricks.jobs import get_run_result

def create_mcp_server(client_manager: ClientManager) -> FastMCP:
    """
    Create and configure the MCP server with all tools.
    
    Args:
        client_manager: Initialized client manager
        
    Returns:
        Configured FastMCP server
    """
    mcp = FastMCP("mcp_unity")
    logger = logging.getLogger(__name__)

    @mcp.tool()
    async def get_schemas(catalog: str):
        """
        Get all schemas and their tables in the workspace for the default catalog.
        """
        try:
            await client_manager.initialize()
            client = client_manager.get_client()
            
            logger.info(f"Getting schemas for catalog: {catalog}")
            schemas = await get_schema_list(
                client,
                catalog=catalog,
                logger=logger
            )
            
            return schemas
        
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
        try:
            await client_manager.initialize()
            client = client_manager.get_client()
            
            logger.info(f"Getting table metadata and sample data for {catalog}.{schema_name}.{table}")

            # Get configuration from client manager
            config = client_manager.config
            workspace_config = config["workspace"]
            
            # Call get_table_sample which now returns only metadata with integrated sample values
            table_metadata = await get_table_sample(
                client=client,
                warehouse_id=workspace_config["warehouse_id"],
                catalog=catalog,
                schema=schema_name,
                table=table,
                sample_size=workspace_config.get("sample_size", 5),
                wait_timeout=workspace_config.get("wait_timeout", "30s"),
                save_table_metadata=workspace_config.get("save_table_metadata", False),
                logger=logger
            )

            # Return the comprehensive metadata dictionary
            return table_metadata
        except Exception as e:
            logger.error(f"Error getting table sample: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    @mcp.tool()
    async def get_schema_metadata_tool(catalog_name: str, schema_name: str):
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
        try:
            await client_manager.initialize()
            client = client_manager.get_client()
            
            schema_metadata = await get_schema_metadata(
                client=client,
                catalog_name=catalog_name,
                schema_name=schema_name,
                logger=logger
            )
            return schema_metadata
        except Exception as e:
            logger.error(f"Error getting schema metadata: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))
        
    @mcp.tool()
    async def get_job_run_result_tool(job_name: str, filter_for_failed_runs: bool = False) -> str:
        """
        Retrieves the results of the last run of a specified Databricks job.

        Args:
            job_name: The name of the Databricks job.
            filter_for_failed_runs: If True, retrieves the result of the last failed run only. 
                                      Defaults to False (retrieves the last completed run regardless of status).

        Returns:
            A string containing the error message, error traceback, and metadata for the selected run.
        """
        try:
            await client_manager.initialize()
            client = client_manager.get_client()

            logger.info(f"Getting run result for job '{job_name}', filter_for_failed_runs={filter_for_failed_runs}")

            # Call the utility function from jobs.py
            run_result_output = await get_run_result(
                job_name=job_name,
                client=client,
                logger=logger,
                filter_for_failed_runs=filter_for_failed_runs
            )

            logger.info(f"Successfully retrieved run result for job '{job_name}'")
            return run_result_output

        except ValueError as ve:
            logger.error(f"Value error getting run result for job '{job_name}': {str(ve)}")
            raise HTTPException(status_code=404, detail=str(ve)) # 404 if job/run not found
        except Exception as e:
            logger.error(f"Error getting run result for job '{job_name}': {type(e).__name__}: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Failed to get run result: {str(e)}")

    return mcp 