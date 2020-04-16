import sys
import os
import pyodbc 
import pandas as pd
import json

from targets.source_sql_server_target import SourceSqlServerTarget
from targets.destination_sql_server_target import DestinationSqlServerTarget

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
        with open(configuration_path, "rb") as file:
            try: 
                configuration = json.loads(file.read())
            except Exception as ex: 
                print(f'extract_load.main: Failed loading configuration {configuration_path}.')
                print(f'extract_load.main: {ex}')


    #TODO: determine what our source is, for now it will only be SQL Server Source
    #TODO: add handling for multiple sources
    source_target = SourceSqlServerTarget(configuration["source"][0]["sql_server_target"])    
    destination_target = DestinationSqlServerTarget(configuration["destination"]["sql_server_target"])

    for table in source_target.get_tables():
        # check change tracking, if change tracking is not enabled add it
        if not source_target.check_change_tracking(table): 
            source_target.add_change_tracking(table)

        destination_target.check_destination_table(table, source_target.get_database)        


if __name__ == "__main__":
    if len(sys.argv) >= 2: 
        if sys.argv[1] != None:
            configuration_path = sys.argv[1]
    else:
        configuration_path = "E:\code\extract_load\configuration.json"

    main(configuration_path)