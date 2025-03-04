import pandas as pd
from src.transform.transform_utils.ingestion_s3_handler import IngestionS3Handler

def transform_data():
    ingention_handler = IngestionS3Handler()
    raw_data = ingention_handler.get_data_from_ingestion()
    df_address = pd.DataFrame(raw_data["address"])
    print(df_address)

transform_data()

#dim_currency -- currency table
    #currency_id INT NN
    #currency_code VARCHAR NN
    #currency_name VARCHAR NN -- missing, to be hardcoded

#dim_date DATE NN -- data missing, need to be created from scratch
    #date_id INT NN
    #year INT NN
    #month INT NN
    #day INT NN
    #day_of_week 
    #day_name VARCHAR NN
    #month_name VARCHAR NN
    #quarter INT NN

#dim_staff -- staff table
    #staff_id INT NN
    #first_name VARCHAR NN
    #last_name VARCHAR NN
    #department_name VARCHAR NN -- from department table
    #location VARCHAR NN -- from department table
    #email_address  EMAIL_Address NN

#dim_design -- design table
    #design_id
    #design_name
    #file_location
    #file_name

#dim_location -- address values??


#dim_counterparty

#fact_sales_order
