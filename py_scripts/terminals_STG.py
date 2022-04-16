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


cursor.execute('drop table s_06_STG_terminals')

cursor.execute('''
 	CREATE TABLE s_06_STG_terminals(
	 	terminal_id varchar(128),
	 	terminal_type varchar(128),
	 	terminal_city varchar(128),
	 	terminal_address varchar(128)
 	)
''')


# # Функция чтения xls файлов
def terminals2sql(path):
	df = pd.read_excel(io = path)
	lst = df.values.tolist()
	# print(lst)
	for sublist in lst:
		cursor.execute('''INSERT INTO s_06_STG_terminals (
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address) 
			VALUES(?, ?, ?, ?)''', sublist)

terminals2sql('terminals_01032021.xlsx')
terminals2sql('terminals_02032021.xlsx')
terminals2sql('terminals_03032021.xlsx')

# cursor.execute('select * from s_06_STG_terminals')
# for row in cursor.fetchall():
# 	print(row)
