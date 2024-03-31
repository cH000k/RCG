# %% [markdown]
# Notes:
# - Primary Keys are considered unique.
# - Any type of database can be used. (used MySQL)
# - Error logging is at API retrieval step
# - 'timestamp' column from API == 'IMPORT_DATE' column on pdf file
# - Columns within DB must have the same name as on what's on the pdf file
# - Columns not listed within the sample pdf are dropped.
# - This is with the assumption that DB tables already exists. (use create_db.py to create tables in MySQL)

# %%
import json
import requests
import datetime
import pandas as pd
from sqlalchemy import create_engine

# %%
user = "root"
password = "root"
host = "localhost"
database = "tzdb"
port = "3306"
engine = None
tbl_tzdb_timezones = "tzdb_timezones"
tbl_tzdb_zone_details = "tzdb_zone_details"
tbl_tzdb_error_log = "tzdb_error_log"
key = "RTMQTUPHJ22S"

tzdb_timezones_url = "http://api.timezonedb.com/v2.1/list-time-zone"
tzdb_timezones_params = {
    "key": key,
    "format": "json"
}

tzdb_zone_details_url = "http://api.timezonedb.com/v2.1/get-time-zone"
tzdb_zone_details_params = {
    "key": key,
    "format": "json",
    "by": "position",
    "lat": 40.689247,
    "lng": -74.044502
}

# %%
def log_error_to_db(msg):
    timestamp = str(datetime.datetime.now())
    data_error = [[timestamp, msg]]
    df = pd.DataFrame(data=data_error, columns=['ERROR_DATE', 'ERROR_MESSAGE'])
    df.to_sql(con=engine, name=tbl_tzdb_error_log, if_exists='append', index=False)
    return print(f"Error has been logged to '{tbl_tzdb_error_log}' table.")

def connect_to_api(url: str, params: dict):
    try:
        response = requests.get(url, params)
        if response.status_code == 200:
            return response
        else:
            msg = f"{response.status_code} {response.raise_for_status()}"
            print(msg)
            log_error_to_db(msg)
    except requests.exceptions.HTTPError as errh:
        msg = f"HTTP Error: {errh}"
        print(msg)
        log_error_to_db(msg)
    except requests.exceptions.ConnectionError as errc:
        msg = f"Connection Error: {errc}"
        print(msg)
        log_error_to_db(msg)
    except requests.exceptions.Timeout as errt:
        msg = f"Timeout Error: {errt}"
        print(msg)
        log_error_to_db(msg)
    except requests.exceptions.RequestException as err:
        msg = f"Unhandled Error: {err}"
        print(msg)
        log_error_to_db(msg)
    
def get_api_dataframe(url: str, params: dict):
    response = connect_to_api(url, params)
    data = json.loads(response.text)

    if data["status"].upper() == "FAILED":
        return print(f"Error: {data['message']} URL and PARAMS mismatch.")
    
    if "get" in url:
        df = pd.DataFrame([data])
        return df
    
    if "list" in url:
        df = pd.DataFrame(data["zones"])
        return df
    
    return ("Please provide a valid API endpoint.")

def convert_datetime_col(df):
    df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.strftime("%m/%d/%Y %H:%M:%S")
    return df

def get_db_connection():
    return create_engine(url=f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}")

def get_rows_to_add(df_out, df_in, tbl_name):
    merge = pd.merge(df_out, df_in, on=['ZONENAME', 'ZONESTART', 'ZONEEND'], how='outer', indicator='exists_yes_no')
    df = merge.loc[merge['exists_yes_no'] == 'right_only']
    df = df[df.columns.drop(list(df.filter(regex='_x')))]
    if len(df) > 0:
        print(f"Table '{tbl_name}' has been updated! {len(df)} new row(s) added.")
    else:
        print(f"Table '{tbl_name}' remains the same! No additional row(s) added.")
    return df

def clean_zone_details_df(df):
    df = df.drop('exists_yes_no', axis=1)
    col_order = ['COUNTRYCODE', 'COUNTRYNAME', 'ZONENAME', 'GMTOFFSET', 'DST', 'ZONESTART', 'ZONEEND', 'IMPORT_DATE']
    true_col_names = {
        'COUNTRYCODE_y':'COUNTRYCODE',
        'COUNTRYNAME_y':'COUNTRYNAME',
        'ZONENAME_y':'ZONENAME',
        'GMTOFFSET_y':'GMTOFFSET',
        'DST_y':'DST',
        'ZONESTART_y':'ZONESTART',
        'ZONEEND_y':'ZONEEND',
        'IMPORT_DATE_y':'IMPORT_DATE'
    }
    df = df.rename(columns=true_col_names) #rename columns
    df = df.reindex(columns=col_order) #reorder columns
    return df

def create_timezone_table(url: str, params: dict):
    df = get_api_dataframe(url, params)
    df = convert_datetime_col(df)
    df.rename(columns= {
        'countryCode':'COUNTRYCODE',
        'countryName':'COUNTRYNAME',
        'zoneName':'ZONENAME',
        'gmtOffset':'GMTOFFSET',
        'timestamp':'IMPORT_DATE'
        }, inplace=True)
    return df

def create_zone_details_table(url: str, params: dict):
    df = get_api_dataframe(url, params)
    df = convert_datetime_col(df)
    df.drop(['status', 'message', 'regionName', 'cityName', 'abbreviation', 'nextAbbreviation', 'formatted'], axis=1, inplace=True)
    df.rename(columns= {
        'countryCode':'COUNTRYCODE',
        'countryName':'COUNTRYNAME',
        'zoneName':'ZONENAME',
        'gmtOffset':'GMTOFFSET',
        'dst':'DST',
        'zoneStart':'ZONESTART',
        'zoneEnd':'ZONEEND',
        'timestamp':'IMPORT_DATE'
        }, inplace=True)
    return df

def select_db_table(tbl_name, engine):
    sql = f"SELECT * FROM {tbl_name}"
    df = pd.read_sql(sql, con=engine)
    return df

# %%

def main():
    try:
        print("Start . . .")
        # Run a GET request from API endpoints to create dataframe
        df_timezone_table = create_timezone_table(tzdb_timezones_url, tzdb_timezones_params)
        df_zone_details_stage = create_zone_details_table(tzdb_zone_details_url, tzdb_zone_details_params)
        
        # Write (re-write) Timezone Table to DB
        df_timezone_table.to_sql(con=engine, name=tbl_tzdb_timezones, if_exists='replace', index=False)
        print(f"Table '{tbl_tzdb_timezones}' has been updated!")

        # FOR TESTING ONLY - dummy data for STAGE dataframe to test appending of data logic
        # df_zone_details_stage = pd.read_csv("test_zone_details.csv") 

        # Read 'tzdb_zone_details' table from DB
        df_zone_details = select_db_table(tbl_tzdb_zone_details, engine)

        # Compare STAGE dataframe vs what's on db, fix column names and arrangement, then add row(s) from STAGE if row(s) does not exist in db
        df_rows_to_add = get_rows_to_add(df_zone_details, df_zone_details_stage, tbl_tzdb_zone_details)
        df_rows_to_add = clean_zone_details_df(df_rows_to_add)

        # Write contents of API directly if "tzdb_zone_details" table is empty
        if len(df_zone_details) == 0:
            df_zone_details_stage.to_sql(con=engine, name=tbl_tzdb_zone_details, if_exists='replace', index=False)
        else:
            df_rows_to_add.to_sql(con=engine, name=tbl_tzdb_zone_details, if_exists='append', index=False)

        print("End . . .")
    except:
        pass

# %%
if __name__ == '__main__':
    engine = get_db_connection()
    main()


