import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv

'''
a function to retrieve the top 5 neighborhoods with the most listings
params:
    cur: a cursor object to execute queries

return None
'''
def get_top_neighborhoods(cur):
    query = """
        SELECT neighbourhood, COUNT(*) as num_listings
        FROM airbnb_listings
        GROUP BY neighbourhood
        ORDER BY numlistings DESC
        LIMIT 5;
    """
    cur.execute(query)
    result = cur.fetchall()
    result_df = pd.DataFrame(result, columns=['neighbourhood', 'num_listings'])
    print("Top 5 neighborhoods with the most listings:")
    print(result_df)

'''
a function to retrieve the host name with the second most listings
params:
    cur: a cursor object to execute queries

return None
'''
def get_host_name_second_max_listings(cur):
    query = """
        SELECT host_name, COUNT(*) as num_listings
        FROM airbnb_listings
        GROUP BY host_name
        ORDER BY num_listings DESC
        OFFSET 1 LIMIT 1;
    """
    cur.execute(query)
    result = cur.fetchall()
    result_df = pd.DataFrame(result, columns=['host_name', 'num_listings'])
    print("Host name with the second most listings:")
    print(result_df)

'''
a function to retrieve the number of listings for each room type in each neighborhood
params:
    cur: a cursor object to execute queries

return None
'''
def get_room_type_listings_each_neighborhood(cur):
    query = """
        SELECT neighbourhood, room_type, COUNT(*) as num_listings
        FROM airbnb_listings
        GROUP BY neighbourhood, room_type;
    """
    cur.execute(query)
    result = cur.fetchall()
    result_df = pd.DataFrame(result, columns=['neighbourhood', 'room_type', 'num_listings'])
    print("Number of listings for each room type in each neighborhood:")
    print(result_df)

'''
a function to analyze the data in the database

return None
'''
def analyze_data():
    load_dotenv()
    user = os.getenv('DB_USER')
    pw = os.getenv('DB_PASS')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')
    database = os.getenv('DB_NAME')

    conn = psycopg2.connect(user=user, password=pw, host=host, port=port, database=database)
    cur = conn.cursor()

    get_top_neighborhoods(cur)
    get_host_name_second_max_listings(cur)
    get_room_type_listings_each_neighborhood(cur)

    cur.close()
    conn.close()