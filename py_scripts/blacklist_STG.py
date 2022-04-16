import jaydebeapi
import sqlite3
import pandas as pd
import csv



connect = jaydebeapi.connect(
'oracle.jdbc.driver.OracleDriver',
'jdbc:oracle:thin:de3hn/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
['de3hn','bilbobaggins'],
'ojdbc7.jar')
cursor = connect.cursor()


cursor.execute('drop table s_06_STG_blacklist')

cursor.execute('''
 	CREATE TABLE s_06_STG_blacklist(
	 	entry_dt timestamp,
	 	passport varchar(128)
 	)
''')


# # Функция чтения xls файлов
def blacklist2sql(path):
	df = pd.read_excel(io = path, dtype ={'entry_dt' : str})
	df = df.astype(str)
	lst = df.values.tolist()
	# print(lst)
	for sublist in lst:
		cursor.execute('''INSERT INTO s_06_STG_blacklist (
			entry_dt, 
			passport) 
			VALUES(
			(to_timestamp(?, 'YYYY-MM-DD HH24:MI:SS')),
			?)''', sublist)

# blacklist2sql('passport_blacklist_01032021.xlsx')
# blacklist2sql('passport_blacklist_02032021.xlsx')
blacklist2sql('passport_blacklist_03032021.xlsx')


# cursor.execute("select * from s_06_STG_blacklist")
# for row in cursor.fetchall():
# 	print(row)
