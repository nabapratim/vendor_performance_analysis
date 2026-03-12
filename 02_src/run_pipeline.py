# run_pipeline.py
# This module is responsible for orchestrating the execution of the data processing pipeline.
# It includes functions to run specific parts of the pipeline, such as KPI extraction and base processing.
# The main function serves as the entry point for the pipeline execution.

from sqlalchemy import create_engine

def run_pipeline(engine, procedure_name):
    with engine.connect() as connection:
        connection.execute(f"CALL {procedure_name}();")
        connection
    print(f"Stored procedure '{procedure_name}' executed successfully.")