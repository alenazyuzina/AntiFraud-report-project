import jaydebeapi
import sqlite3
import pandas as pd
import csv
import os
import glob
import sys

connect = jaydebeapi.connect(
'oracle.jdbc.driver.OracleDriver',
'jdbc:oracle:thin:de3hn/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
['de3hn','bilbobaggins'],
'ojdbc7.jar')

sys.path.insert(0, 'py_scripts')

cursor = connect.cursor()

def rename(old_name, new_name):
	os.rename(old_name, new_name)

rename("passport_blacklist_01032021.xlsx", "./archive/passport_blacklist_01032021.xlsx.backup")
rename("passport_blacklist_02032021.xlsx", "./archive/passport_blacklist_02032021.xlsx.backup")
rename("passport_blacklist_03032021.xlsx", "./archive/passport_blacklist_03032021.xlsx.backup")
rename("terminals_01032021.xlsx", "./archive/terminals_01032021.xlsx.backup")
rename("terminals_02032021.xlsx", "./archive/terminals_02032021.xlsx.backup")
rename("terminals_03032021.xlsx", "./archive/terminals_03032021.xlsx.backup")
rename("transactions_01032021.txt", "./archive/transactions_01032021.txt.backup")
rename("transactions_02032021.txt", "./archive/transactions_02032021.txt.backup")
rename("transactions_03032021.txt", "./archive/transactions_03032021.txt.backup")



