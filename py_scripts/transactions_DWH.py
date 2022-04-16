import jaydebeapi
import sqlite3


connect = jaydebeapi.connect(
'oracle.jdbc.driver.OracleDriver',
'jdbc:oracle:thin:de3hn/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
['de3hn','bilbobaggins'],
'ojdbc7.jar')

cursor = connect.cursor()

cursor.execute('drop table s_06_DWH_FACT_transactions')

cursor.execute('''
	CREATE TABLE s_06_DWH_FACT_transactions(
		transaction_id varchar(128) primary key,
		transaction_date timestamp,
		amount decimal,
		card_num varchar(128),
		oper_type varchar(128),
		oper_result varchar(128),
		terminal varchar(128),
		create_dt timestamp default current_timestamp
	)
''')

cursor.execute(''' 
	INSERT INTO s_06_DWH_FACT_transactions(
		transaction_id,
		transaction_date,
		amount,
		card_num,
		oper_type,
		oper_result,
		terminal
	) SELECT
		transaction_id,
		transaction_date,
		amount,
		card_num,
		oper_type,
		oper_result,
		terminal
	FROM s_06_STG_transactions
''')

# cursor.execute('select * from s_06_DWH_FACT_transactions where rownum <= 10')
# result = cursor.fetchall()
# for row in result:
# 	print(row)