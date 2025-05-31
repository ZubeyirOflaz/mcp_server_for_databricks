"""Databricks authentication utilities."""

import logging
import subprocess
import json
from datetime import datetime

async def databricks_login(host: str, refresh_token: bool = False) -> bool:
    """
    Perform Databricks login using the CLI with the mcp_server_for_databricks profile.
    
    Args:
        host: Databricks workspace URL
        refresh_token: Whether this is a token refresh operation
        
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

def get_databricks_token(host: str) -> tuple[str, datetime]:
    """
    Get the Databricks access token and expiry time.
    
    Args:
        host: Databricks workspace URL
        
    Returns:
        Tuple of (access_token, expiry_datetime)
        
    Raises:
        ValueError: If token cannot be retrieved or parsed
    """
    profile_name = "mcp_server_for_databricks"
    
    try:
        token_output = subprocess.check_output(
            ["databricks", "auth", "token", "--host", host, "--profile", profile_name]
        ).decode("utf-8").strip()
        logging.info(f"Successfully retrieved auth token using profile: {profile_name}")
        
        # Parse the JSON output
        token_data = json.loads(token_output)
        access_token = token_data.get("access_token")
        token_expiry_datetime = datetime.fromisoformat(token_data.get("expiry"))
        
        if not access_token:
            logging.error("Failed to extract access_token from token response")
            logging.error(f"Token response: {token_output}")
            raise ValueError("Could not extract access_token from Databricks token response")
        
        logging.info("Successfully extracted access_token")
        return access_token, token_expiry_datetime
        
    except json.JSONDecodeError:
        logging.error(f"Failed to parse token output as JSON: {token_output}")
        raise ValueError("Invalid JSON response from databricks auth token command")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to get token: {e}")
        raise ValueError(f"Failed to retrieve Databricks token: {e}") 