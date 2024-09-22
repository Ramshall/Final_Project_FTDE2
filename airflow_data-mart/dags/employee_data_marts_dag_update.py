
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession

# PostgreSQL connection details
jdbc_url = "jdbc:postgresql://35.213.145.33:5432/db_hr"
connection_properties = {
    "user": "nbt_finalproject",
    "password": "123nbt123",
    "driver": "org.postgresql.Driver"
}

# Spark session initialization
def get_spark_session():
    spark = SparkSession.builder \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0") \
        .master("local") \
        .appName("PySpark_Postgres") \
        .getOrCreate()
    return spark

# Function to create mart_demografi_employees
def create_mart_demografi_employees():
    spark = get_spark_session()
    dim_employee_df = spark.read.jdbc(url=jdbc_url, table="warehouse.dim_employee", properties=connection_properties)
    dim_employee_df.createOrReplaceTempView("dim_employee")

    mart_demografi_employees = spark.sql("""
        SELECT
            EmployeeID,
            Name,
            Gender,
            Age,
            Department,
            Position
        FROM dim_employee
    """)

    mart_demografi_employees.write.mode("overwrite").option("header", "true").csv("mart_demografi_employees.csv")
    mart_demografi_employees.write.jdbc(url=jdbc_url, table="data_mart.demografi_employees", mode="overwrite", properties=connection_properties)

# Function to create mart_biaya_sdm
def create_mart_biaya_sdm():
    spark = get_spark_session()
    fact_payroll_df = spark.read.jdbc(url=jdbc_url, table="warehouse.fact_payroll", properties=connection_properties)
    fact_payroll_df.createOrReplaceTempView("fact_payroll")

    mart_biaya_sdm = spark.sql("""
        SELECT
            PaymentDate,
            EmployeeID,
            Name,
            Salary,
            OvertimePay,
            (Salary + OvertimePay) as TotalPayroll
        FROM fact_payroll
    """)

    mart_biaya_sdm.write.mode("overwrite").option("header", "true").csv("mart_biaya_sdm.csv")
    mart_biaya_sdm.write.jdbc(url=jdbc_url, table="data_mart.biaya_sdm", mode="overwrite", properties=connection_properties)

# Function to create mart_employee_training_result
def create_mart_employee_training_result():
    spark = get_spark_session()
    fact_training_df = spark.read.jdbc(url=jdbc_url, table="warehouse.fact_training", properties=connection_properties)
    fact_training_df.createOrReplaceTempView("fact_training")

    mart_employee_training_result = spark.sql("""
        SELECT
            EmployeeID,
            Name,
            TrainingProgram,
            StartDate,
            EndDate,
            Status,
            CASE
                WHEN Status = 'Completed' THEN DATEDIFF(EndDate, StartDate)
                ELSE NULL
            END AS DaysTaken
        FROM fact_training
    """)

    mart_employee_training_result.write.mode("overwrite").option("header", "true").csv("mart_employee_training_result.csv")
    mart_employee_training_result.write.jdbc(url=jdbc_url, table="data_mart.employee_training_result", mode="overwrite", properties=connection_properties)

# Function to create mart_performance
def create_mart_performance():
    spark = get_spark_session()
    fact_performance_df = spark.read.jdbc(url=jdbc_url, table="warehouse.fact_performance", properties=connection_properties)
    fact_performance_df.createOrReplaceTempView("fact_performance")
   
    fact_payroll_df = spark.read.jdbc(url=jdbc_url,table="warehouse.fact_payroll",properties=connection_properties)
    fact_payroll_df.createOrReplaceTempView("fact_payroll")

    mart_performance_employees = spark.sql("""
    SELECT
        fp.EmployeeID,
        fp.Name,
        SPLIT_PART(fp.ReviewPeriod, ' ', 1) AS Quarter,
        SPLIT_PART(fp.ReviewPeriod, ' ', 2) AS Year,
        fp.Rating,
        CASE
            WHEN fp.Rating > 4.5 THEN 'Very good performance'
            WHEN fp.Rating > 4 AND fp.Rating <= 4.5 THEN 'Excellent Performance'
            WHEN fp.Rating > 3 THEN 'Good performance'
            WHEN fp.Rating <= 3 THEN 'Needs Improvement'
        END AS New_Comments,
        CAST((p.Salary + p.OvertimePay) AS INT) AS total_payroll
    FROM
        fact_performance fp
    LEFT JOIN fact_payroll p ON fp.EmployeeID = p.EmployeeID
""")

    mart_performance_employees.write.mode("overwrite").option("header", "true").csv("mart_performance.csv")
    mart_performance_employees.write.jdbc(url=jdbc_url, table="data_mart.performance", mode="overwrite", properties=connection_properties)

# DAG definition
with DAG(
    dag_id="employee_data_marts_dag",
    start_date=datetime(2024, 9, 21),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    create_demografi_task = PythonOperator(
        task_id="create_mart_demografi_employees",
        python_callable=create_mart_demografi_employees
    )

    create_biaya_sdm_task = PythonOperator(
        task_id="create_mart_biaya_sdm",
        python_callable=create_mart_biaya_sdm
    )

    create_employee_training_result_task = PythonOperator(
        task_id="create_mart_employee_training_result",
        python_callable=create_mart_employee_training_result
    )

    create_performance_task = PythonOperator(
        task_id="create_mart_performance",
        python_callable=create_mart_performance
    )

    # Task dependencies
    create_demografi_task >> create_biaya_sdm_task >> create_employee_training_result_task >> create_performance_task
