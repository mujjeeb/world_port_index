# Import statements
import pandas as pd
import subprocess
import math
from sqlalchemy import create_engine

# Functions to read and transform the .mdb file

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

# Get values for the target row(Singapore's JURONG ISLAND port(country ='SG',port_name = 'JURONG ISLAND'))
singapore_port = wpi_data_df[wpi_data_df["Main_port_name"] =='"JURONG ISLAND"']

#Reducing the columns to contain only port name, latitudde and longitude, so we can calculate the distance from singapore's port
selected_columns = ["Main_port_name", "Latitude_degrees","Longitude_degrees"]
singapore_df=wpi_data_df[selected_columns]

# Drop N/A values (Values with type "Nonetype")
singapore_df=singapore_df.dropna()

# Loop through rows to calculate the distance 
distance_arr=[]
for index, row in singapore_df.iterrows():
    lat1 = singapore_port["Latitude_degrees"]
    long1 =singapore_port["Longitude_degrees"]
    lat2 = int(row["Latitude_degrees"])
    long2 = int(row["Longitude_degrees"])
    distance = haversine_distance(lat1, long1, lat2, long2)
    distance_arr.append(distance)
# Create distance column in the Dataframe 
singapore_df["distance_in_m"] = distance_arr


#Sort dataframe based on the distance column
column_to_sort_by="distance_in_m"
sorted_singapore_df = singapore_df.sort_values(by=column_to_sort_by,ascending=True)

# drop the row with Main_port_name == "JURONG ISLAND"
sorted_singapore_df =sorted_singapore_df[sorted_singapore_df["Main_port_name"] != '"JURONG ISLAND"']
closest_port_df = sorted_singapore_df.head(5)

# Connecting the engine to the pre-established table
engine = create_engine('postgresql://postgres:[password]@localhost:5432/gofrieghts_db')
closest_port_df.to_sql('closest_ports_to_Jurong_Island_port', engine, if_exists='append')

