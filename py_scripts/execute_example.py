import jaydebeapi
import sqlite3


connect = jaydebeapi.connect(
'oracle.jdbc.driver.OracleDriver',
'jdbc:oracle:thin:de3hn/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
['de3hn','bilbobaggins'],
'ojdbc7.jar')

cursor = connect.cursor()

cursor.execute('select * from s_06_REP_FRAUD')
result = cursor.fetchall()
for row in result:
	print(row)	