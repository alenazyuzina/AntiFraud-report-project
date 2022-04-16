import jaydebeapi
import sqlite3
import pandas as pd
import csv
import datetime
import jpype
import jpype.imports
# import java


connect = jaydebeapi.connect(
'oracle.jdbc.driver.OracleDriver',
'jdbc:oracle:thin:de3hn/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
['de3hn','bilbobaggins'],
'ojdbc7.jar')
cursor = connect.cursor()


cursor.execute('drop table s_06_STG_transactions')




cursor.execute('''
 	CREATE TABLE s_06_STG_transactions(
	 	transaction_id varchar(128) primary key,
	 	transaction_date timestamp,
	  	amount decimal,
		card_num varchar(128),
		oper_type varchar(128),
		oper_result varchar(128),
		terminal varchar(128)
 	)
''')

# Функция чтения тхт файлов
def csv2sql(path):
	df = pd.read_csv(path, delimiter = ';', decimal=',', dtype ={'amount' : float})
	df = df.astype(str)
	# df['transaction_date'] = df['transaction_date'].strftime()
	df.columns = df.columns.str.strip()
	df['transaction_id'] = df['transaction_id'].str.strip()
	df['transaction_date'] = df['transaction_date'].str.strip()
	df['card_num'] = df['card_num'].str.strip()
	df['oper_type'] = df['oper_type'].str.strip()
	df['oper_result'] = df['oper_result'].str.strip()
	df['terminal'] = df['terminal'].str.strip()
	# df['transaction_date'] = pd.to_datetime(df['transaction_date'])

	# print(df)
	lst = df.values.tolist()
	# print(lst)
	for sublist in lst:
		cursor.execute('''
			INSERT INTO s_06_STG_transactions (
			transaction_id,
			transaction_date,
			amount,
			card_num,
			oper_type,
			oper_result,
			terminal) 
			VALUES(
			? , (to_timestamp(?,'YYYY-MM-DD HH24:MI:SS')), (to_number(?, '99999.99')), ?, ?, ?, ?)''', sublist)

csv2sql('transactions_01032021.txt')
csv2sql('transactions_02032021.txt')
csv2sql('transactions_03032021.txt')


# cursor.execute("select count(*) from s_06_STG_transactions")
# for row in cursor.fetchall():
# 	print(row)

