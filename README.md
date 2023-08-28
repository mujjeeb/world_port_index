# Extract Load (EL) pipeline in Python to transition the World Port Index data from the Access database to PostgreSQL.

## Project Overview

This project creates an Extract Load (EL) pipeline in python on the World Port Index data fromm Access database to PostgreSQL. It contauns three different python scripts that:
1. Find the closest port to  Jurong Island port in singapore
2. Finds the country with the highest number of ports with cargo wharfs
3. Finds the closest port to a particular point given its Longitude and Latitude with provisions, water, fuel, and diesel. This could be used by ships in distress.



## How it Works

1. **Data Extraction**: The python script extracts data from the WPI access database(The access databases is provided). The Databases contains a few tables but the tables 'wpi Data' and 'Country Codes' were the only ones used by the three scripts.
2. **Data Transformation**: The extracted data is then transformed to a dataframe to allow for easy manipulation.

3. **Data Loading**: The trandormed data is securely loaded into PostgreSQL database by each script. The PostgreSQL database acts as a centralized repository for easy access and analysis.


## Requirements

- Python 3.x
- PG4 Admin
- Pandas
- SQLAlchemy
- PostgreSQL

## Usage

1. Start up PG4 Admin and then establish a database named 'gofrieghts_db'. All the tables will be loaded to this database

2. Run the jupyter notebooks

3. Create a connection to the created table by inputing your PG4 Admin password in the connection section of the python script



## Contribution and Feedback

Contributions are welcomed to help uncover more insights into the data and deepen my knowledge of Python and Postgre SQL. Feel free to raise issues, or provide feedback to help us improve the project.


## Github profile
https://github.com/mujjeeb