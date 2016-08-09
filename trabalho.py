import sqlite3
import xmltodict
import numpy as np
import pandas as pd

if pd.__version__ != '0.18.1':
    print "Esse script necessita do pandas == 0.18.1"
    exit()

con = sqlite3.connect("./database.sqlite")
matches = pd.read_sql_query("SELECT * from Match", con)
