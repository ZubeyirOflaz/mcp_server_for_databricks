"""Configuration data models."""

from typing import Optional
from pydantic import BaseModel, Field

class WorkspaceConfig(BaseModel):
    """Databricks workspace configuration."""
    url: str = Field(..., description="Databricks workspace URL")
    warehouse_id: str = Field(..., description="SQL warehouse ID")
    warehouse_name: str = Field(..., description="SQL warehouse name")
    sample_size: int = Field(default=5, description="Default sample size for table data")
    catalog: Optional[str] = Field(default=None, description="Default catalog name")
    profile: Optional[str] = Field(default="mcp_server_for_databricks", description="Databricks CLI profile name")
    wait_timeout: Optional[str] = Field(default="30s", description="Query wait timeout")
    save_table_metadata: Optional[bool] = Field(default=False, description="Whether to save table metadata to files")

class AppConfig(BaseModel):
    """Application configuration."""
    workspace: WorkspaceConfig 