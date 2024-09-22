import pandas as pd
from connection import create_connection, logger

def load_data(df, engine, table_name):
    # Memuat data tanpa mengubah nama kolom menjadi huruf kecil
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    logger.info(f"Loaded {len(df)} rows to {table_name} in data lake")

def etl_to_database():
    try:
        # create connection to database
        mysql_engine = create_connection('35.213.145.33', 3306, 'training_employees', 'nbt_finalproject', '123nbt123', 'mysql')
        postgres_engine = create_connection('35.213.145.33', 5432, 'postgres', 'nbt_finalproject', '123nbt123')
        
        # extract data from csv to dataframe
        df_training_dev = pd.read_csv('.\\data\\data_training_development_update.csv') 
        df_performance = pd.read_csv('.\\data\\data_performance_management_update.csv')
        df_payroll = pd.read_csv('.\\data\\data_management_payroll_update.csv')
        
        # transform: drop duplicates without changing column names to lowercase
        df_training_dev = df_training_dev.drop_duplicates()
        df_performance = df_performance.drop_duplicates()
        df_payroll = df_payroll.drop_duplicates()
        
        # load to database
        load_data(df_training_dev, mysql_engine, 'training_development')
        load_data(df_performance, postgres_engine, 'performance_management')
        load_data(df_payroll, postgres_engine, 'management_payroll')
        
    except Exception as e:
        logger.error(f"ETL to data lake process failed: {str(e)}")
        raise

if __name__ == "__main__":
    etl_to_database()
