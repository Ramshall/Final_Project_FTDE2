# Final_Project_FTDE2

Final Project: ETL for Human Capital Analytics Dashboard
Team Members:

* Gusti Imam Saputra
* Muhammad Ramadhani
* Nindy Nova Pratiwi S
* Suyanto

## Porject Overview
This project focuses on building a Human Capital Analytics Dashboard System designed to support various human resources functions, including:

1. Recruitment
2. Payroll
3. Training and Development
4. Performance Management

The data is stored across multiple databases, including MongoDB, PostgreSQL, and MySQL, each serving different functionalities. Our goal was to provide a comprehensive, real-time view of HR metrics for better decision-making.

## Business Use Case
The dashboard provides insights into key human resource metrics:
* Demographic Information: Employee numbers, gender distribution, and age range.
* Job Applicant Data: Number of applicants, gender, age range, and potential applicants using machine learning predictions.
* Human Resource Costs: Salaries and overtime costs.
* Employee Performance: Performance trends and predictions for low-performing employees.

## Data Sources and Tools
* Recruitment & Selection: Data stored in MongoDB.
* Data Management & Payroll: Stored in PostgreSQL.
* Training & Development: Data stored in MySQL.
* Performance Management: Employee performance data stored in PostgreSQL.

Our data pipeline utilizes Apache Kafka for streaming data as both a producer and consumer, Apache Airflow for orchestrating and scheduling the ETL processes on data mart, PostgreSQL as the Data Warehouse for managing and storing structured data, and Google Data Studio for creating and visualizing the final dashboard. 

## Machine Learning Prediction
Using machine learning, we predicted Potential Job Applicants. It's Determining likely candidates for hiring or rejection.
The prediction results were stored in PostgreSQL for analysis and report generation.

## Project Workflow
![image](https://github.com/user-attachments/assets/b6c3b560-dfe3-49f1-92ba-a98156eb725f)
The workflow involves the following steps:
1. Importing CSV data into PostgreSQL and MySQL.
2. Setting up Kafka Producer and Consumer for real-time data ingestion from recruitment systems.
3. Designing an ERD for the Data Warehouse and Data Marts.
4. Scheduling ETL Jobs using Airflow.
5. Building the dashboard in Google Data Studio.

## Conclusion & Future Work
![image](https://github.com/user-attachments/assets/1be21e0d-e5f5-4d90-a19e-be74b4514d67)
**Key Metrics:**
Employee Demographics: Majority of employees are aged 41-50. Gender distribution is nearly equal.
Employee Metrics: Total number of employees: 556. Total overtime pay: 1.4 million IDR. Total salary: 47.8 million IDR.
Performance Trends: Performance has remained consistent across quarters.

**Future Work:**
We aim to further enhance the model predictions and extend the dashboard with additional HR metrics.

## Other Repository link
* [Google Drive](https://drive.google.com/drive/folders/1yqXweMOHtmGtctLSbRYE7_gNMX2je5GE?usp=sharing)
* [Dashboard](https://lookerstudio.google.com/u/0/reporting/e4c22ddd-1a2e-4b9c-9559-94ff392032e9/page/GjgCE)

