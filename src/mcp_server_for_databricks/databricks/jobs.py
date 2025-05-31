"""Databricks job operations."""

import logging
import asyncio
from typing import Optional
from databricks.sdk import WorkspaceClient

async def get_run_result(
    job_name: str, 
    client: WorkspaceClient, 
    logger: Optional[logging.Logger] = None, 
    filter_for_failed_runs: bool = False
) -> str:
    """
    Retrieves the results of a Databricks job.
    
    Args:
        job_name: Name of the job to retrieve results for
        client: Authenticated WorkspaceClient instance
        logger: Logger instance to use
        filter_for_failed_runs: If True, only failed runs will be included
    
    Returns:
        Result of the last run of the job (if filter_for_failed_runs is True, only last failed run will be sent)
    
    Raises:
        ValueError: If job or runs are not found
        Exception: For other unexpected errors
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    try:
        # Find the job ID
        job_id = await asyncio.to_thread(
            client.jobs.list,
            name=job_name,
            limit=1
        )

        if not job_id:
            logger.error(f"No job found with name: {job_name}")
            raise ValueError(f"No job found with name: {job_name}")
        
        job_id = list(job_id)[0].job_id

        # Get the run ID
        run_list = await asyncio.to_thread(
            client.jobs.list_runs,
            job_id=job_id,
            completed_only=True,
        )
        
        run_id = None
        if not filter_for_failed_runs:
            run_id = list(run_list)[0].run_id
        else:
            for run in run_list:
                if run.state.result_state.value == "FAILED":
                    run_id = run.run_id
                    break
            if run_id is None:
                logger.error(f"No failed runs found for job: {job_name}")
                raise ValueError(f"No failed runs found for job: {job_name}")
        
        if run_id is None:
            logger.error(f"No runs found for job: {job_name}")
            raise ValueError(f"No runs found for job: {job_name}")
        
        # Get the run result
        run_result = await asyncio.to_thread(
            client.jobs.get_run,
            run_id=run_id
        )
        
        last_failed_id = None
        last_timestamp = -999
        for task in run_result.tasks:
            if task.end_time > last_timestamp:
                if not filter_for_failed_runs or task.state.result_state.value == 'FAILED':
                    last_failed_id = task.run_id
                    last_timestamp = task.end_time
        
        run_result_output = await asyncio.to_thread(
            client.jobs.get_run_output,
            run_id=last_failed_id
        )
        
        error_message = f"Error message: {run_result_output.error}\nError traceback: {run_result_output.error_trace}\nMetadata: {run_result_output.metadata.as_dict()}"
        
        return error_message
        
    except ValueError as e:
        logger.error(f"Value error getting run result: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error getting run result: {str(e)}")
        raise 