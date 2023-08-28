""" 
This script takes a Longitude and Latitude of any point on earth and then provides the longitude and Latitude of the
closest port with provisions(water,fuel oil and diesel). For the sake of testing we would use lat: 32.610982, long: -38.706256.
""" 

# Latitude and the Longitude of the given point.
DISTRESS_LAT = 32.610982
DISTRESS_LONG = -38.706256


# Import statements
import pandas as pd
import subprocess
import math
from sqlalchemy import create_engine

# Functions to read .mdb file
def show_data(path=path, table=table):
    tables = subprocess.check_output(["mdb-export", path, table])
    return tables.decode().split('\n')
 
def convert_df(path, table):
    d = show_data(path, table)
    columns = d[0].split(',')
    data = [i.split(',') for i in d[1:]]
    df = pd.DataFrame(columns=columns, data=data)
    return df

# Function to calculate the distance between two points given their longitude and Latitude

def haversine_distance(lat1, long1, lat2, long2):
    # Convert degrees to radians
    lat1 = math.radians(lat1)
    long1 = math.radians(long1)
    lat2 = math.radians(lat2)
    long2 = math.radians(long2)
    
    # Earth's radius in kilometers
    R = 6371.0
    
    # Differences in coordinates
    dlat = lat2 - lat1
    dlong = long2 - long1
    
    # Haversine formula
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlong / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    result = R * c * 1000
    
    distance = round(result,2)
    return distance


# Read the table wpi data and then convert it to a dataframe
path = 'WPI.mdb'
table = 'wpi Data'
wpi_data_df = convert_df(path,table)


#Drop the row with none values 
wpi_data_df=wpi_data_df.dropna()


# Read the table 'country code' and then convert it to a dataframe
path = 'WPI.mdb'
country_table = 'Country Codes'
country_list = show_data(path,country_table)

# Transforming the data gotten to turn it into a df
# Creating a list of list to make turning into DF easier
new_list = []
for item in (country_list):
    new_item = []
    new_item.append(item)
    new_list.append(new_item)
del new_list[-1]
# Creating a DataFrame
country_df = pd.DataFrame(new_list, columns=['Sentences'])

# Splitting the Sentences column into two columns
country_df[['Code', 'Country']] = country_df['Sentences'].str.split('","',expand=True)

#Strip both sides for  "
country_df['Code'] = df['Code'].str.strip('"')
country_df['Country'] = df['Country'].str.rstrip('"')

# Dropping the original Sentences column
country_df.drop(columns=['Sentences'], inplace=True)

# Drop the last and the first rows as they are not useful data(i.e are neither countries nor codes) 
country_df.drop([0], axis=0, inplace=True)

# Create a column in wpi_data_df called country and initialize it to be empty
wpi_data_df['Country'] = ''

# Perform lookup and populate the country_column
for index1, row1 in wpi_data_df.iterrows():
    temp1 = (row1['Wpi_country_code'] ).strip('"')
    match_found = False  # Flag to check if a match is found
    for index2, row2 in df.iterrows():
        temp2 = row2['Code']
        if  temp1 == temp2:
            wpi_data_df.at[index1, 'Country'] = row2['Country']
            match_found = True  # Set the flag to True
    if not match_found:
        wpi_data_df.at[index1, 'Country'] = 'Unknown'  # Add a default value if no match is found, this is done as inspection of the data showed that 'PM' is missing on both country codes tables provided


#Selecting the columns that would be needed
selected_columns = ['Supplies_water', 'Supplies_fuel_oil', 'Supplies_diesel_oil','Supplies_provisions','Latitude_degrees', 'Longitude_degrees' ,'Country','Main_port_name']
unfiltered_df = wpi_data_df[selected_columns]

# Strip the columns of the '"'
unfiltered_df['Supplies_water'] = unfiltered_df['Supplies_water'].str.strip('"')
unfiltered_df['Supplies_fuel_oil'] = unfiltered_df['Supplies_fuel_oil'].str.strip('"')
unfiltered_df['Supplies_diesel_oil'] = unfiltered_df['Supplies_diesel_oil'].str.strip('"')
unfiltered_df['Supplies_provisions'] = unfiltered_df['Supplies_provisions'].str.strip('"')


# Check the possible values for the columns needed to filter the ports
unfiltered_df['Supplies_diesel_oil'].unique()

# make a copy of the dataframe 
filtered_df = unfiltered_df.copy()

#filter for the required amenities
filtered_df = filtered_df[filtered_df['Supplies_water'] == 'Y']
filtered_df = filtered_df[filtered_df['Supplies_fuel_oil'] == 'Y']
filtered_df = filtered_df[filtered_df['Supplies_diesel_oil'] == 'Y']
filtered_df = filtered_df[filtered_df['Supplies_provisions'] == 'Y']


# Loop through rows to calculate the distance 
distance_arr=[]
for index, row in filtered_df.iterrows():
    lat1 = DISTRESS_LAT
    long1 = DISTRESS_LONG
    lat2 = int(row["Latitude_degrees"])
    long2 = int(row["Longitude_degrees"])
    distance = haversine_distance(lat1, long1, lat2, long2)
    distance_arr.append(distance)
# Create distance column in the Dataframe 
filtered_df["distance_in_m"] = distance_arr

# Sort the values in the 'distance_in_m' column to get the closest ports
filtered_sorted_df = filtered_df.sort_values(by='distance_in_m', ascending=True)

# the columns for the final dataframe are country, port_name, port_latitude and port_longitude
result_columns = ['Country','Port_name','Port_latitude','Port_longitude']

# Rename columns
column_mapping = {
    'Main_port_name': 'Port_name',
    'Latitude_degrees': 'Port_latitude',
    'Longitude_degrees': 'Port_longitude',
}

filtered_sorted_df.rename(columns=column_mapping, inplace=True)
filtered_sorted_df

closest_port_df_unfiltered = filtered_sorted_df.head(1)

closest_port_df= closest_port_df_unfiltered[result_columns]
 
#Drop the index 
closest_port_df.set_index('Country', inplace=True)

# Connecting the engine to the pre-established table
engine = create_engine('postgresql://postgres:[password]1@localhost:5432/gofrieghts_db')
closest_port_df.to_sql('closest_port_to_a_point', engine, if_exists='append')