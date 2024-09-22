import pandas as pd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from connection import create_connection
import logging
from sqlalchemy import text

logger = logging.getLogger(__name__)

def execute_sql_file(cursor, sql_file):
    with open(sql_file, 'r') as file:
        sql_content = file.read()
    try:
        cursor.execute(sql_content)
        logger.info(f"SQL file executed successfully")
    except Exception as e:
        logger.error(f"Error executing SQL file: {e}")
        raise

def read_sql(engine, query):
    try:
        df = pd.read_sql(text(query), engine)
        logger.info(f"Data read successfully using query: {query}")
        return df
    except Exception as e:
        logger.error(f"Error reading SQL: {e}")
        raise

def write_to_sql(df, engine, table_name, schema=None, if_exists='append'):
    try:
        # Menulis data ke SQL tanpa mengubah nama kolom
        df.to_sql(table_name, engine, schema=schema, if_exists=if_exists, index=False)
        logger.info(f"Data written successfully to {schema}.{table_name}")
    except Exception as e:
        logger.error(f"Error writing to SQL: {e}")
        raise

def etl_process():
    mysql_engine = create_connection('35.213.145.33', 3306, 'training_employees', 'nbt_finalproject', '123nbt123', 'mysql')
    postgres_engine = create_connection('35.213.145.33', 5432, 'postgres', 'nbt_finalproject', '123nbt123', 'postgres')
    warehouse_engine = create_connection('35.213.145.33', 5432, 'db_hr', 'nbt_finalproject', '123nbt123', 'postgres')
    
    # Use psycopg2 for initial schema and table creation
    warehouse_conn = psycopg2.connect(
        host='35.213.145.33',
        port=5432,
        database='db_hr',
        user='nbt_finalproject',
        password='123nbt123'
    )
    warehouse_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    
    try:
        with warehouse_conn.cursor() as cursor:
            # Execute the SQL file to create schema and tables
            execute_sql_file(cursor, './query/dwh_design.sql')
        
        # ETL for dim_employee
        query_employee = '''
        SELECT 
            "EmployeeID", 
            "Name", 
            "Gender", 
            "Age", 
            "Department", 
            "Position"
        FROM public.management_payroll
        '''
        df_employee = read_sql(postgres_engine, query_employee)
        write_to_sql(df_employee, warehouse_engine, 'dim_employee', schema='warehouse')

        # ETL for fact_performance
        query_performance = '''
        SELECT 
            "EmployeeID",
            "Name", 
            "ReviewPeriod", 
            "Rating", 
            "Comments"
        FROM public.performance_management
        '''
        df_performance = read_sql(postgres_engine, query_performance)
        write_to_sql(df_performance, warehouse_engine, 'fact_performance', schema='warehouse')

        # ETL for fact_payroll
        query_payroll = '''
        SELECT 
            "EmployeeID",
            "Name", 
            "Salary", 
            "OvertimePay", 
            "PaymentDate"
        FROM public.management_payroll
        '''
        df_payroll = read_sql(postgres_engine, query_payroll)
        write_to_sql(df_payroll, warehouse_engine, 'fact_payroll', schema='warehouse')

        # ETL for fact_training (MySQL)
        query_training = '''
        SELECT 
            EmployeeID,
            Name, 
            TrainingProgram, 
            StartDate, 
            EndDate, 
            Status
        FROM training_development
        '''
        df_training = read_sql(mysql_engine, query_training)
        write_to_sql(df_training, warehouse_engine, 'fact_training', schema='warehouse')

        logger.info("ETL process completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred during ETL: {str(e)}")
        raise

    finally:
        warehouse_conn.close()
        
if __name__ == "__main__":
    etl_process()