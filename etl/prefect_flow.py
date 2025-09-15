from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner
import subprocess
import time
from .pipeline import run_etl_pipeline
from .logger import setup_logger

logger = setup_logger(__name__)

@task(retries=2, retry_delay_seconds=60, log_prints=True)
def extract_transform_load_task():
    
    logger.info("Starting ETL task with Prefect")
    result = run_etl_pipeline()
    
    if not result['success']:
        raise Exception(f"ETL failed: {result.get('error', 'Unknown error')}")
    
    logger.info(f"ETL completed: {result['records_processed']} records in {result['duration_seconds']}s")
    return result

@task(retries=1, retry_delay_seconds=30, log_prints=True)
def run_dbt_transformations_task():
    
    logger.info("Starting dbt transformations with Prefect")
    
    result = subprocess.run(
        ["dbt", "run", "--select", "staging", "intermediate", "marts"], 
        cwd="dbt", 
        capture_output=True, 
        text=True
    )
    
    if result.returncode != 0:
        logger.error(f"dbt failed: {result.stderr}")
        raise Exception(f"dbt transformations failed: {result.stderr}")
    
    logger.info("dbt transformations completed")
    return {"success": True, "output": result.stdout}

@flow(
    name="crypto-etl-pipeline",
    description="Complete crypto data pipeline with Prefect orchestration",
    task_runner=SequentialTaskRunner(),
    log_prints=True
)
def crypto_prefect_flow():
        
    logger.info("="*50)
    logger.info("STARTING PREFECT ORCHESTRATION CRYPTO PIPELINE")
    logger.info("="*50)
    
    start_time = time.time()
    
    try:
        # Step 1: ETL Pipeline
        etl_result = extract_transform_load_task()
        
        # Step 2: dbt Transformations  
        dbt_result = run_dbt_transformations_task()
        
        # Success metrics
        duration = time.time() - start_time
        
        result = {
            "success": True,
            "etl_records": etl_result['records_processed'],
            "dbt_success": dbt_result['success'],
            "total_duration": round(duration, 2)
        }
        
        logger.info("="*50)
        logger.info("PREFECT PIPELINE COMPLETED")
        logger.info(f"Total duration: {duration:.2f} seconds")
        logger.info("="*50)
        
        return result
        
    except Exception as e:
        duration = time.time() - start_time
        logger.error("="*50)
        logger.error("PREFECT PIPELINE FAILED")
        logger.error(f"Error: {str(e)}")
        logger.error(f"Duration: {duration:.2f} seconds")
        logger.error("="*50)
        raise

if __name__ == "__main__":
    result = crypto_prefect_flow()
    print(f"Pipeline completed: {result}")# Updated: Mon Sep 15 13:54:14 +08 2025
