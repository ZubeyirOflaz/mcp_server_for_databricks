# MCP Server for Databricks Interaction

## 1. Project Aim

This project provides a server application that acts as an interface to a Databricks workspace. It utilizes the FastMCP framework to expose tools that allow users to query and retrieve information about Databricks resources like schemas, tables, samples, and job results directly through MCP commands.

The primary goal is to simplify common Databricks metadata retrieval tasks for users interacting via the MCP interface, leveraging the Databricks SDK and CLI for backend communication.

## 2. Installation and Initialization

### Prerequisites

*   Python (version 3.x recommended)
*   `uv` (Python package installer and virtual environment manager). You can install it following the instructions at https://github.com/astral-sh/uv.
*   `databricks-cli` installed and accessible in your system\'s PATH.

### Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository_url>
    cd <repository_directory>
    ```
2.  **Create a virtual environment and install dependencies:**
    ```bash
    uv venv  # Create a virtual environment (e.g., .venv)
    uv sync  # Install dependencies from pyproject.toml and uv.lock
    source .venv/bin/activate # Activate the virtual environment (use `.venv\\Scripts\\activate` on Windows)
    ```

### Initialization

Before running the server for the first time, you need to configure its connection to your Databricks workspace:

1.  **Run the initialization script:**
    ```bash
    python init.py
    ```
2.  **Follow the prompts:**
    *   Enter your Databricks **workspace URL** (e.g., `https://adb-xxxxxxxxxxxx.azuredatabricks.net`).
    *   The script will initiate the Databricks CLI **OAuth login flow** using the profile name `mcp_server_for_databricks`. Follow the instructions provided by the CLI in your terminal/browser to authenticate.
    *   Select an available **SQL Warehouse** from the list provided. This warehouse will be used for executing metadata queries.
    *   Enter the desired **sample size** (number of rows) to retrieve when using the table sampling tool. Press Enter to use the default (5).
3.  **Configuration Saved:** The script will save the workspace URL, selected warehouse ID/name, and sample size into a `config.yaml` file in the project root.

## 3. How it Works

*   **Server Framework:** Uses `FastMCP` to define tools and handle communication via standard input/output (`stdio`).
*   **Databricks Connection:** Interacts with Databricks using the `databricks-sdk` for Python.
*   **Authentication:**
    *   Relies on a `databricks-cli` profile named `mcp_server_for_databricks`.
    *   The `main.py` script uses `subprocess` to call `databricks auth login` and `databricks auth token` to obtain and use authentication tokens for the SDK client. Authentication happens during the initial server startup or on the first tool call if the server hasn't initialized yet.
*   **Information Retrieval:**
    *   **Metadata Queries:** Most schema and table listing operations are performed by executing SQL statements (`SHOW DATABASES`, `SHOW TABLES`) via the `client.statement_execution.execute_statement` SDK method against the configured SQL Warehouse.
    *   **Detailed Table Info:** Fetches comprehensive table details (column types, comments, properties, etc.) using the `client.tables.get` SDK method.
    *   **Table Sampling:** Retrieves sample data by executing a `SELECT * ... LIMIT N` query via the `client.statement_execution.execute_statement` method.
    *   **Job Results:** Uses the `client.jobs` API (`list`, `list_runs`, `get_run`, `get_run_output`) to find jobs and retrieve output from specific runs.
*   **Configuration:** Reads connection details (workspace URL, warehouse ID, sample size) from `config.yaml` upon startup.

## 4. MCP Tools Provided

The following tools are exposed by this server:

*   **`get_schemas(catalog: str)`**
    *   **Description:** Retrieves a list of all schemas and the tables contained within each schema for a specified catalog.
    *   **Arguments:**
        *   `catalog` (str): The name of the Databricks catalog to query.
    *   **Returns:** A list of objects, each containing `catalog`, `schema_name`, and a list of `tables`.

*   **`get_table_sample_tool(catalog: str, schema_name: str, table: str)`**
    *   **Description:** Returns detailed metadata for a specific table, including column information (type, comment, nullability) and a sample number of rows from the table data. The number of sample rows is determined by the `sample_size` configured during initialization. Optionally saves the metadata and sample data to the `.input_data/` directory if configured in `config.yaml`.
    *   **Arguments:**
        *   `catalog` (str): The catalog name.
        *   `schema_name` (str): The schema name.
        *   `table` (str): The table name.
    *   **Returns:** A dictionary containing detailed table metadata with an added `sample_values` list for each column.

*   **`get_schema_metadata(catalog_name: str, schema_name: str)`**
    *   **Description:** Retrieves metadata for a specific schema, including the schema comment and details for each table within it (table comment, creation timestamp, table type, owner).
    *   **Arguments:**
        *   `catalog_name` (str): The catalog name.
        *   `schema_name` (str): The schema name.
    *   **Returns:** A dictionary containing `schema_comment` and a nested `tables` dictionary with metadata for each table.

*   **`get_job_run_result(job_name: str, filter_for_failed_runs: bool = False)`**
    *   **Description:** Retrieves the results from the most recent run of a specified Databricks job. Provides options to get the latest run regardless of status or specifically the *last failed* run.
    *   **Arguments:**
        *   `job_name` (str): The exact name of the Databricks job.
        *   `filter_for_failed_runs` (bool, optional): If `True`, retrieves the result of the last failed run only. Defaults to `False` (retrieves the last completed run).
    *   **Returns:** A string containing the error message, error traceback, and metadata associated with the selected job run.
