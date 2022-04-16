import jaydebeapi
import sqlite3


connect = jaydebeapi.connect(
'oracle.jdbc.driver.OracleDriver',
'jdbc:oracle:thin:de3hn/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
['de3hn','bilbobaggins'],
'ojdbc7.jar')

cursor = connect.cursor()

cursor.execute('drop table s_06_DWH_FACT_pssprt_blcklst')

cursor.execute('''
	CREATE TABLE s_06_DWH_FACT_pssprt_blcklst(
	 	entry_dt timestamp,
	 	passport varchar(128),
	 	create_dt timestamp default current_timestamp,
	 	udpate_dt timestamp 
 	) 
''')

cursor.execute('''
	INSERT INTO s_06_DWH_FACT_pssprt_blcklst(
	 	entry_dt,
	 	passport
	) SELECT
		entry_dt,
	 	passport
	 FROM s_06_STG_blacklist
 ''')

# cursor.execute('select * from s_06_DWH_FACT_pssprt_blcklst')
# result = cursor.fetchall()
# for row in result:
# 	print(row)