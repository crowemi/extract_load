import sys
import os
import pyodbc 
import pandas as pd
import json
import logging
import queue
import threading

from targets.source_sql_server_target import SourceSqlServerTarget
from targets.destination_sql_server_target import DestinationSqlServerTarget
                    
# df = pd.read_sql(sql, conn)
# json = df.loc[0].to_json()
# print(json)

#TODO: add logging
def main(configuration_path=None):
    configuration = None

    if not configuration_path == None: 
        with open(configuration_path, "rb") as file:
            try: 
                configuration = json.loads(file.read())
            except Exception as ex: 
                print(f'extract_load.main: Failed loading configuration {configuration_path}.')
                print(f'extract_load.main: {ex}')

    source_targets = []
    destination_target = DestinationSqlServerTarget(configuration["destination"]["server"], configuration["destination"]["database"], configuration["destination"]["schema"], configuration["destination"]["load_psa"])

    for target in configuration["source"]:
        for table in target["tables"]:
            source_targets.append(SourceSqlServerTarget(target["server"], target["database"], target["schema"], table))

    for target in source_targets:
        process_source_target(target, destination_target)

    #     # get records new thread which loads the queue 
    #     get_record_thread = threading.Thread(source_target.get_records())
    #     get_record_thread.join()


def process_source_target(source_target, destination_target):
    # check change tracking, if change tracking is not enabled add it 
    if not source_target.check_change_tracking(): 
        source_target.add_change_tracking()

    # get the current change version
    current_change_version = source_target.get_new_change_tracking_key()
    # create shell record for this change, we'll use the ID later to update the shell record flag completion of load
    current_change_log_id = destination_target.set_change_tracking_key(source_target.get_table_name, source_target.get_database_name, current_change_version)
    # get the previous change version 
    previous_change_version = destination_target.get_previous_change_tracking_key(source_target.get_table_name, source_target.get_database_name)

    # check that the destination tables exist, both stage and psa - if either doesn't exist create them
    if not destination_target.check_stg_destination_table(source_target.get_table_name, source_target.get_database) and destination_target.check_psa_destination_table(source_target.get_table_name, source_target.get_database):
        destination_target.create_destination_table(source_target.get_table_name, source_target.get_database)


if __name__ == "__main__":
    if len(sys.argv) >= 2: 
        if sys.argv[1] != None:
            configuration_path = sys.argv[1]
    else:
        configuration_path = "E:\code\extract_load\configuration.json"

    main(configuration_path)