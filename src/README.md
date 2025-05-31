# MCP Server for Databricks - Refactored Structure

This directory contains the refactored version of the MCP Server for Databricks with improved project structure and separation of concerns.

## Project Structure

```
src/
├── main.py                                    # Entry point script
└── mcp_server_for_databricks/                # Main package
    ├── __init__.py                           # Package initialization
    ├── main.py                               # Main application entry point
    ├── app.py                                # Application orchestration
    ├── config/                               # Configuration management
    │   ├── __init__.py
    │   ├── models.py                         # Pydantic configuration models
    │   └── loader.py                         # Configuration loading and validation
    ├── auth/                                 # Authentication management
    │   ├── __init__.py
    │   ├── databricks_auth.py               # Databricks CLI authentication
    │   └── token_manager.py                 # Token lifecycle management
    ├── client/                               # Databricks client management
    │   ├── __init__.py
    │   └── manager.py                        # WorkspaceClient lifecycle
    ├── databricks/                           # Databricks operations
    │   ├── __init__.py
    │   ├── schemas.py                        # Schema operations
    │   ├── tables.py                         # Table operations
    │   └── jobs.py                           # Job operations
    ├── mcp_tools/                            # MCP server and tools
    │   ├── __init__.py
    │   └── registry.py                       # MCP tool registration
    ├── models/                               # Data models
    │   └── __init__.py
    └── utils/                                # Utility functions
        ├── __init__.py
        └── logging.py                        # Logging configuration
```

## Key Improvements

### 1. **Separation of Concerns**
- **Configuration**: Isolated in `config/` module with proper validation
- **Authentication**: Dedicated `auth/` module for token management
- **Client Management**: Centralized in `client/` module
- **Databricks Operations**: Organized by functionality in `databricks/`
- **MCP Tools**: Separated into `mcp_tools/` module

### 2. **Better Error Handling**
- Centralized error handling in each module
- Proper exception propagation
- Detailed logging throughout

### 3. **Improved Maintainability**
- Clear module boundaries
- Reduced coupling between components
- Easier testing and debugging

### 4. **Enhanced Extensibility**
- Easy to add new Databricks operations
- Simple to extend MCP tools
- Modular authentication system

## Usage

### Running the Server
```bash
# From the src directory
python main.py

# Or directly from the package
python -m mcp_server_for_databricks.main
```

### Importing Components
```python
from mcp_server_for_databricks.app import MCPDatabricksApp
from mcp_server_for_databricks.client.manager import ClientManager
from mcp_server_for_databricks.config.loader import load_config
```

## Module Descriptions

### `config/`
- **`models.py`**: Pydantic models for configuration validation
- **`loader.py`**: Configuration loading from YAML with validation

### `auth/`
- **`databricks_auth.py`**: Handles Databricks CLI authentication
- **`token_manager.py`**: Manages token lifecycle and expiry

### `client/`
- **`manager.py`**: Manages WorkspaceClient initialization and refresh

### `databricks/`
- **`schemas.py`**: Schema listing and metadata operations
- **`tables.py`**: Table sampling and metadata operations
- **`jobs.py`**: Job run result retrieval

### `mcp_tools/`
- **`registry.py`**: MCP server creation and tool registration

### `utils/`
- **`logging.py`**: Centralized logging configuration

## Migration from Original Structure

The original monolithic files have been refactored as follows:

- **`main.py`** → Split into `app.py`, `main.py`, and various modules
- **`utils.py`** → Split into `databricks/`, `config/`, and `utils/`
- **`init.py`** → Remains unchanged (initialization script)

## Benefits

1. **Modularity**: Each component has a single responsibility
2. **Testability**: Individual modules can be tested in isolation
3. **Maintainability**: Changes are localized to specific modules
4. **Readability**: Clear structure makes the codebase easier to understand
5. **Extensibility**: New features can be added without affecting existing code

## Next Steps

1. Add comprehensive unit tests for each module
2. Implement proper dependency injection
3. Add configuration validation schemas
4. Create integration tests
5. Add performance monitoring and metrics 