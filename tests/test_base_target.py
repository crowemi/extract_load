import sys, os
import unittest
import logging
import multiprocessing
import pyodbc
import pandas as pd

import extract_load
from sources.extract_sql_server import SqlServerTarget 

class TestExtractLoad(unittest.TestCase):
    def setUp(self):
        self.process_name = 'TESTPROCESSNAME'
        self.vendor_name = 'TESTVENDORNAME'