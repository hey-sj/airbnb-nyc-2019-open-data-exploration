import pandas as pd

'''
a function to clean the data and save it to a new file.
this function drops duplicate rows, `host_id`, `name`, and `last_review` columns, drops rows with missing `host_name`,
fills missing values in `reviews_per_month` with 0, and converts string columns to categorical columns.
it also filters out rows with `price` values outside the range of 0 to 5500 (this range is arbitrary and can be adjusted).
it also filters out rows with `minimum_nights` values greater than 365 (a full year's stay). 
it also filters out rows with `availability_365` values outside the range of 1 to 365
finally, it saves the cleaned data to a new file.

params: input_path: str, path to the input file
        output_path: str, path to save the cleaned file
        
return None
'''
def clean_data(input_path, output_path):
    data = pd.read_csv(input_path)
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
    data.to_csv(output_path, index=False)