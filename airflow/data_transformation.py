import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

'''
a function to transform the data and save it to a new table in the database (replacing the old)
params:
    input_path: str, path to the input file

return None
'''
def transform_data(input_path):
    load_dotenv()
    user = os.getenv('DB_USER')
    pw = os.getenv('DB_PASS')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')
    database = os.getenv('DB_NAME')

    engine = create_engine(f'postgresql://{user}:{pw}@{host}:{port}/{database}')
    data = pd.read_csv(input_path)
    data.to_sql('airbnb_listings', con=engine, if_exists='replace', index=False)