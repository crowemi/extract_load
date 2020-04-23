import sys
import os
import pyodbc 
import pandas as pd
import json
import logging
from queue import Queue
import threading
import multiprocessing
from collections import namedtuple

from targets.source_sql_server_target import SourceSqlServerTarget
from targets.destination_sql_server_target import DestinationSqlServerTarget
                    
# df = pd.read_sql(sql, conn)
# json = df.loc[0].to_json()
# print(json)

#TODO: add logging
def process_source_start():
    print("start")

def process_source_target(input):
    
    source_target = SourceSqlServerTarget(input[0], input[1], input[2], input[3])
    destination_target = DestinationSqlServerTarget(input[4]["server"], input[4]["database"], input[4]["schema"], input[4]["load_psa"])

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
    if not destination_target.check_stg_destination_table(source_target.get_table_name, source_target.get_database_name) and destination_target.check_psa_destination_table(source_target.get_table_name, source_target.get_database_name):
        destination_target.create_destination_table(source_target.get_table_name, source_target.get_database_name)


if __name__ == "__main__":
    if len(sys.argv) >= 2: 
        if sys.argv[1] != None:
            configuration_path = sys.argv[1]
    else:
        configuration_path = r"E:\code\extract_load\configuration.json"

    if not configuration_path == None: 
        with open(configuration_path, "rb") as file:
            try: 
                configuration = json.loads(file.read())
            except Exception as ex: 
                print(f'extract_load.main: Failed loading configuration {configuration_path}.')
                print(f'extract_load.main: {ex}')

    # sets max concurrent processes based on configuration else defaults to 4
    pool_size = 1 #multiprocessing.cpu_count() * 2
    pool = multiprocessing.Pool(pool_size, initializer=process_source_start)
    
    source_target_processes = []
    data = list()

    for target in configuration["source"]:
        for table in target["tables"]:
            data.append((target["server"], target["database"], target["schema"], table, configuration["destination"]))
            # create each individual processes for each source target, we can then throttle based on machine cores. Arguements must be picklable (serializable/deserializable)
            # source_target_processes.append(multiprocessing.Process(target=process_source_target, args=(target["server"], target["database"], target["schema"], table, configuration["destination"]), daemon=True))
    
    pool.map(process_source_target, data)
    pool.close()
    pool.join()
    
