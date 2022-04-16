import jaydebeapi
import sqlite3


connect = jaydebeapi.connect(
'oracle.jdbc.driver.OracleDriver',
'jdbc:oracle:thin:de3hn/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
['de3hn','bilbobaggins'],
'ojdbc7.jar')

cursor = connect.cursor()

cursor.execute('drop table s_06_DWH_FACT_terminals')

cursor.execute('''
	CREATE TABLE s_06_DWH_FACT_terminals(
		terminal_id varchar(128),
		terminal_type varchar(128),
		terminal_city varchar(128),
		terminal_address varchar(128),
		create_dt timestamp default current_timestamp,
		update_dat timestamp
	)
''')

cursor.execute('''
	INSERT INTO s_06_DWH_FACT_terminals(
		terminal_id,
		terminal_type,
		terminal_city,
		terminal_address
	) SELECT
		terminal_id,
		terminal_type,
		terminal_city,
		terminal_address
	FROM s_06_STG_terminals
''')

# cursor.execute('select * from s_06_DWH_FACT_terminals')
# result = cursor.fetchall()
# for row in result:
# 	print(row)