# Introduction 
This application is used to automate the extract and load process for structured and semi-structured data sources. The following data sources are loadable: 

    Microsoft SQL Server
    Microsoft Excel File ["xls", "xlsx", "xlsm", "xlsb", "obf"]

# Configuration

Configuration of the application is performed via JSON. The configuration details are outlined below in the master nodes. 

### Logging
    "logging" : {
        "file_name" : "",
        "file_path" : "",
        "log_level" : ""
    }

- file_name (string): the name of the logging file. A datetime stamp is added to the end of the file name. 
- file_path (string): the path where the logging file will be saved. 
- log_level (string): the level of logging required for the current execution. [DEBUG, INFO, WARN]

### Source

#### Microsoft SQL Server Source
    {
      "type": "sql",
      "server" : string,
      "database" : string,
      "schema" : string, 
      "tables" : [
        { "name": string }
      ]
    }

#### Excel Source
    {
        "type" : "excel",
        "files" : [
            { 
                "vendor_name" : string,
                "process_name" : string,
                "path" : string, 
                "psa_batch_size" : int,
                "archive_file" : bool,
                "archive_file_path" : string,
                "delete_file" : bool,
                "primary_key" : [
                    {}
                ],
                "first_row_header" : bool,
                "skip_rows" : int
            }
        ]
    }


### Destination
    "destination" : {
        "server" : "",
        "database" : "",
        "schema" : "",
        "load_psa" : 
    }

- server (string):  

### Data Type Conversions

# TODO
  - Add configurable MSSQL drivers 
  - Make logging level configurable, and validate input 