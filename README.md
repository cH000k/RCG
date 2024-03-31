Name: Noel Cedrix O. Joaquin

GOAL:
1. Create a python script to query the TimezoneDB API and populate the tables TZDB_TIMEZONES and TZDB_ZONE_DETAILS
2. Your script should handle errors when retrieving the API and log them into the table TZDB_ERROR_LOG.
3. TZDB_TIMEZONES is to be deleted every time you script runs and populated with data from the API.
4. TZDB_ZONE_DETAILS is to be populated incrementally, not adding rows if the data in the table already exists.

Notes:
• To get access to the API provided by TimezoneDB go to: https://timezonedb.com and create an account (free), you will be provided with a Key that you can use in your scripts.
• You can use your preferred database to upload the data.
