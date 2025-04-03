import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, Table, MetaData, select, update, insert, Column, Integer, String, Float
import psycopg2
import os

# load environment variables
load_dotenv()

# get environment variables
user = os.getenv('DB_USER')
pw = os.getenv('DB_PASS')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
database = os.getenv('DB_NAME')

# connect to postgresql server
conn = psycopg2.connect(dbname='postgres', user=user, password=pw, host=host, port=port)
conn.autocommit = True
cur = conn.cursor()

# create the database if it does not exist
try:
    cur.execute(f'CREATE DATABASE "{database}"')
except psycopg2.errors.DuplicateDatabase:
    print(f"Database {database} already exists.")
finally:
    cur.close()
    conn.close()

# now connect to the new database
engine = create_engine(f'postgresql://{user}:{pw}@{host}:{port}/{database}')
connection = engine.connect()

# create metadata
metadata = MetaData()

# define the table schema
table = Table(
    'airbnb_listings', metadata,
    Column('id', Integer, primary_key=True),
    Column('neighbourhood_group', String),
    Column('neighbourhood', String),
    Column('latitude', Float),
    Column('longitude', Float),
    Column('room_type', String),
    Column('price', Integer),
    Column('minimum_nights', Integer),
    Column('number_of_reviews', Integer),
    Column('reviews_per_month', Float),
    Column('calculated_host_listings_count', Integer),
    Column('availability_365', Integer)
)

# Create the table if it does not exist
metadata.create_all(engine)

'''
function will update the table with data from the dataframe.
if the record already exists, it will update the record.
if the record does not exist, it will insert a new record.

:param data: pandas dataframe
:param table: sqlalchemy table object
:param connection: sqlalchemy connection object

return None
'''
def update_table(data, table, connection):
    batch_size = 1000
    for start in range(0, len(data), batch_size):
        batch = data.iloc[start:start + batch_size]
        for index, row in batch.iterrows():
            query = select(table.c.id).where(table.c.id == row['id'])
            result = connection.execute(query).fetchone()

            if result:  # Update existing record
                query = (
                    update(table).where(table.c.id == row['id'])
                    .values(
                        neighbourhood_group=row['neighbourhood_group'],
                        neighbourhood=row['neighbourhood'],
                        latitude=row['latitude'],
                        longitude=row['longitude'],
                        room_type=row['room_type'],
                        price=row['price'],
                        minimum_nights=row['minimum_nights'],
                        number_of_reviews=row['number_of_reviews'],
                        reviews_per_month=row['reviews_per_month'],
                        calculated_host_listings_count=row['calculated_host_listings_count'],
                        availability_365=row['availability_365']
                    )
                )
            else:  # Insert new record
                query = (
                    insert(table)
                    .values(
                        id=row['id'],
                        neighbourhood_group=row['neighbourhood_group'],
                        neighbourhood=row['neighbourhood'],
                        latitude=row['latitude'],
                        longitude=row['longitude'],
                        room_type=row['room_type'],
                        price=row['price'],
                        minimum_nights=row['minimum_nights'],
                        number_of_reviews=row['number_of_reviews'],
                        reviews_per_month=row['reviews_per_month'],
                        calculated_host_listings_count=row['calculated_host_listings_count'],
                        availability_365=row['availability_365']
                    )
                )
            connection.execute(query)

# load the data
data = pd.read_csv('data/ABNB_NYC_2019.csv')

# data cleaning and processing steps done in the ipynb
data.drop_duplicates(inplace=True)
data.drop(['host_id', 'name'], axis=1, inplace=True)
data.dropna(subset=['host_name'], inplace=True)
data.drop('last_review', axis=1, inplace=True)
data.fillna({'reviews_per_month': 0}, inplace=True)
string_cols = ['host_name', 'neighbourhood_group', 'neighbourhood', 'room_type']
for col in string_cols:
    data[col] = data[col].astype('category')
data = data[(data['price'] > 0) & (data['price'] < 5500)]
data = data[data['minimum_nights'] <= 365]
data = data[(data['availability_365'] > 0) & (data['availability_365'] <= 365)]

# perform batch processing
update_table(data, table, connection)

# insert data into the database
data.to_sql('airbnb_listings', con=engine, if_exists='replace', index=False)

# close connection
connection.close()