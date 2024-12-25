import pandas as pd
from sqlalchemy import create_engine,text
import cx_Oracle
from Scripts.logg import setup_logger
from Scripts.config import  *

# Create mysql engine
#mysql_engine = create_engine('mysql+pymysql://root:Admin%40143@localhost:3308/retaildwh')

mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')

logger=setup_logger()

# When we use insert,update delete we have to use this in such a way
def load_fact_sales():
    query = text("""insert into fact_sales(sales_id,product_id,store_id,quantity,total_sales,sale_date)
                     select sales_id,product_id,store_id,quantity,total_amount,sale_date from sales_with_deatils;""")
    try:
        with mysql_engine.connect() as connection:
            logger.info("Executing load_fact_sales function to load the fact_sales table")
            connection.execute(query)
            logger.info("load_fact_sales: Data loaded successfully")
    except Exception as e:
        logger.error("load_fact_sales: Data loading failed", exc_info=True)
def  load_inventory_fact():
    query = text("""insert into fact_inventory select * from staging_inventory;""")
    try:
        with mysql_engine.connect() as conn:
            logger.info("Executing load_inventory_fact function to load the fact_inventory table")
            conn.execute(query)
            conn.commit()
    except Exception as e:
        logger.error('load_inventory_fact: Data loding failed')
        logger.info("An error occured while executing query",e,exc_info=True)

def  load_inventory_levels_by_store():
    query = text("""insert into inventory_levels_by_store(store_id,total_inventory) select store_id,total_inventory from aggregated_inventory_levels;""")
    try:
        with mysql_engine.connect() as conn:
            logger.info("Executing load_inventory_levels_by_store function to load the inventory_levels_by_store table")
            conn.execute(query)
            conn.commit()
    except Exception as e:
        logger.info("An error occured while executing query",e,exc_info=True)


def  load_Monththly_summary():
    query = text("""insert into monthly_sales_summary select * from monthly_sales_summary_source;""")
    try:
        with mysql_engine.connect() as conn:
            logger.info("Executing load_inventory_fact function to load the fact_inventory table")
            conn.execute(query)
            conn.commit()
    except Exception as e:
        logger.info("An error occured while executing query",e,exc_info=True)


if __name__== '__main__':
    logger.info("Data load strted ....")
    load_fact_sales()
    load_inventory_fact()
    load_inventory_levels_by_store()
    load_Monththly_summary()
    logger.info("Data load finsihed ....")

