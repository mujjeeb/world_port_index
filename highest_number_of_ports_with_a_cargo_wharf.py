# Import statements
import pandas as pd
import subprocess
import math
from io import StringIO
from sqlalchemy import create_engine

# Functions to read and tranform .mdb file

def show_data(path=path, table=table):
    tables = subprocess.check_output(["mdb-export", path, table])
    return tables.decode().split('\n')
 
def convert_df(path, table):
    d = show_data(path, table)
    columns = d[0].split(',')
    data = [i.split(',') for i in d[1:]]
    df = pd.DataFrame(columns=columns, data=data)
    return df

# Read the table wpi data and then convert it to a dataframe
path = 'WPI.mdb'
table = 'wpi Data'
wpi_data_df = convert_df(path,table)

# Transforming the country table to a dataframe
#Drop the row with none values 
new_df=wpi_data_df.dropna()

# Read the table country code and then convert it to a dataframe
path = 'WPI.mdb'
country_table = 'Country Codes'
country_list = show_data(path,country_table)

# Creating a list of list to make turning into DF 
new_list = []
for item in (country_list):
    new_item = []
    new_item.append(item)
    new_list.append(new_item)
del new_list[-1]

# Creating a DataFrame
df = pd.DataFrame(new_list, columns=['Sentences'])

# Splitting the Sentences column into two columns
df[['Code', 'Country']] = df['Sentences'].str.split('","',expand=True)

#Strip both sides for  "
df['Code'] = df['Code'].str.strip('"')
df['Country'] = df['Country'].str.rstrip('"')

# Dropping the original Sentences column
df.drop(columns=['Sentences'], inplace=True)


# Drop the last and the first rows as they are not useful data(i.e are neither countries nor codes) 
df.drop([0], axis=0, inplace=True)


# Create a column in new_df called country and initialize it to be empty
new_df['Country'] = ''

# Perform lookup and populate the country_column
for index1, row1 in new_df.iterrows():
    temp1 = (row1['Wpi_country_code'] ).strip('"')
    match_found = False  # Flag to check if a match is found
    for index2, row2 in df.iterrows():
        temp2 = row2['Code']
        if  temp1 == temp2:
            new_df.at[index1, 'Country'] = row2['Country']
            match_found = True  # Set the flag to True
    if not match_found:
        new_df.at[index1, 'Country'] = 'Unknown'  # Add a default value if no match is found, this is done as inspection of the data showed that 'PM' is missing on both country codes tables provided

# Drop columns all other columns except for 'Country' as its the only column we would use to count
selected_columns = ['Country']
df_cargo_wharf = new_df[selected_columns]
df_cargo_wharf


# Group by country and count the number of ports with cargo_wharf
country_port_count = df_cargo_wharf.groupby('Country').value_counts().reset_index()


country_port_count.columns = ["Country","Port_Count"] 

# Find the country with the largest number of ports with cargo_wharf
max_ports_country = country_port_count[country_port_count['Port_Count'] == country_port_count['Port_Count'].max()]

#Drop the index 
max_ports_country.set_index('Country', inplace=True)
max_ports_country

# Connecting the engine to the pre-established table
engine = create_engine('postgresql://postgres:mkhitaryan1@localhost:5432/gofrieghts_db')
max_ports_country.to_sql('countries_with_largest_number_of ports_with_cargo_wharfs', engine, if_exists='append')
