from sqlalchemy import create_engine
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_connection(host, port, database, user, password, db_type='postgres'):
    try:
        if db_type == 'mysql':
            conn_string = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'
        elif db_type == 'postgres':
            conn_string = f'postgresql://{user}:{password}@{host}:{port}/{database}'
        else:
            raise ValueError("Unsupported database type")
        
        engine = create_engine(conn_string)
        logger.info(f"Connection to {db_type} database {database} created successfully")
        return engine
    except Exception as e:
        logger.error(f"Error creating connection: {e}")
        raise

def close_connection(engine):
    if engine:
        engine.dispose()
        logger.info("Connection closed")