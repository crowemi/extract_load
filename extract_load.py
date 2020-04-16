import sys
import os
import pyodbc 
import pandas as pd
import json

from targets.sql_server_target import SqlServerTarget

#  
# 1. Get the requested source tables from list (database or configuration)
#   a. Does a stage table exist in destination?
#       i. if not, create stable table in destination 
#   b. Does change tracking exist?
#       i. if not, add change tracking to table
#   c. Has the application run before? 
#       i. if not, get change key, run "full load"
#      ii.
# 2. 
#                    

# conn = pyodbc.connect('DRIVER={SQL Server}; SERVER=DEVSQL17TRZ3; DATEBASE=hpXr_db; Trusted_Connection=yes')
# sql = "SELECT TOP 10 * FROM facets.dbo.CMC_CLCL_CLAIM"

# df = pd.read_sql(sql, conn)
# json = df.loc[0].to_json()
# print(json)



def main(configuration_path=None):
    configuration = None

    if not configuration_path == None: 
        with open(os.path(configuration_path), "rb") as file:
            try: 
                configuration = json.loads(file.read())
            except Exception as ex: 
                print(f'extract_load.main: Failed loading configuration {configuration_path}.')
                print(f'extract_load.main: {ex}')

    # determin what our source
    sql_server_target = SqlServerTarget(configuration)    


if __name__ == "__main__":
    if not sys.argv[1] == None:
        configuration_path = sys.argv[1]
    else:
        configuration_path = "configuration.json"

    main(configuration_path)