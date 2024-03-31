# %%
import mysql.connector

user = "root"
password = "root"
host = "localhost"
database = "tzdb"
port = "3306"
tbl_tzdb_timezones = "tzdb_timezones"
tbl_tzdb_zone_details = "tzdb_zone_details"
tbl_tzdb_error_log = "tzdb_error_log"

# %% [markdown]
# # CREATE MYSQL SERVER CONNECTION

# %%
def create_server_connection(user: str, password: str, host: str, database: str):
    con = None
    try:
        con = mysql.connector.connect(user=user, password=password, host=host, database=database)
        print("MySQL DB Connection Successful!")
    except Exception as e:
        print("Error Connecting to Database:")
        print(e)
    return con

con = create_server_connection(user, password, host, database)
cursor = con.cursor()

# %% [markdown]
# # CREATE/DROP DATABASE

# %%
def create_database(con, db_name):
    cursor = con.cursor()
    cursor.execute(f"CREATE DATABASE {db_name}")
    return print(f"{db_name} has been created!")

def delete_database(con, db_name):
    cursor = con.cursor()
    cursor.execute(f"DROP DATABASE {db_name}")
    return print(f"{db_name} has been deleted!")

# %% [markdown]
# # CREATE/DROP TABLE - TZDB_TIMEZONES

# %%
# CREATE
try:
    cursor.execute(f"""
        CREATE TABLE {tbl_tzdb_timezones} (
                    COUNTRYCODE VARCHAR(2) NOT NULL,
                    COUNTRYNAME VARCHAR(100) NOT NULL,
                    ZONENAME VARCHAR(100) NOT NULL,
                    GMTOFFSET NUMERIC,
                    IMPORT_DATE DATE,
                    PRIMARY KEY(ZONENAME)
                    )           
    """)
    print(f"Table {tbl_tzdb_timezones} created!")
except:
    print(f"Table '{tbl_tzdb_timezones}' already exists!")

# DROP
# try:
#     cursor.execute(f"DROP TABLE {tbl_tzdb_timezones}")
#     print(f"Table {tbl_tzdb_timezones} dropped!")
# except:
#     print(f"Table '{tbl_tzdb_timezones}' no longer exists!")

# %% [markdown]
# # CREATE/DROP TABLE - TZDB_ZONE_DETAILS

# %%
# CREATE
try:
    cursor.execute(f"""
        CREATE TABLE {tbl_tzdb_zone_details} (
                    COUNTRYCODE VARCHAR(2) NOT NULL,
                    COUNTRYNAME VARCHAR(100) NOT NULL,
                    ZONENAME VARCHAR(100) NOT NULL,
                    GMTOFFSET NUMERIC NOT NULL,
                    DST NUMERIC NOT NULL,
                    ZONESTART NUMERIC NOT NULL,
                    ZONEEND NUMERIC NOT NULL,
                    IMPORT_DATE DATE,
                    PRIMARY KEY(ZONENAME, ZONESTART, ZONEEND)
                    )         
    """)
    print(f"Table {tbl_tzdb_zone_details} created!")
except:
    print(f"Table '{tbl_tzdb_zone_details}' already exists!")

# DROP
# try:
#     cursor.execute(f"DROP TABLE {tbl_tzdb_zone_details}")
#     print(f"Table {tbl_tzdb_zone_details} dropped!")
# except:
#     print(f"Table '{tbl_tzdb_zone_details}' no longer exists!")

# %% [markdown]
# # CREATE/DROP TABLE - TZDB_ERROR_LOG

# %%
# CREATE
try:
    cursor.execute(f"""
        CREATE TABLE {tbl_tzdb_error_log} (
                    ERROR_DATE DATE NOT NULL,
                    ERROR_MESSAGE VARCHAR(1000) NOT NULL
                    )         
    """)
    print(f"Table {tbl_tzdb_error_log} created!")
except:
    print(f"Table '{tbl_tzdb_error_log}' already exists!")

# DROP
# try:
#     cursor.execute(f"DROP TABLE {tbl_tzdb_error_log}")
#     print(f"Table {tbl_tzdb_error_log} dropped!")
# except:
#     print(f"Table '{tbl_tzdb_error_log}' no longer exists!")


