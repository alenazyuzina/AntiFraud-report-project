import jaydebeapi
import sqlite3


connect = jaydebeapi.connect(
'oracle.jdbc.driver.OracleDriver',
'jdbc:oracle:thin:de3hn/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
['de3hn','bilbobaggins'],
'ojdbc7.jar')

cursor = connect.cursor()

cursor.execute(''' 
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE s_06_REP_FRAUD';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;
''')

cursor.execute(''' 
	CREATE TABLE s_06_REP_FRAUD(
		event_dt timestamp,
		passport varchar(128),
		fio varchar (128),
		phone varchar(128),
		event_type varchar(128),
		report_dt timestamp default current_timestamp
		)
''')

# Инсерт данных по первому типу мошенничества
cursor.execute('''
	INSERT INTO s_06_REP_FRAUD(
		event_dt,
		passport,
		fio,
		phone,
		event_type
	)SELECT
		event_dt,
		passport,
		fio,
		phone,
		event_type
	FROM fraud_1_tmp
''')

# Инсерт данных по второму типу мошенничества

cursor.execute('''
	INSERT INTO s_06_REP_FRAUD(
		event_dt,
		passport,
		fio,
		phone,
		event_type
	)SELECT
		event_dt,
		passport,
		fio,
		phone,
		event_type
	FROM fraud_2_tmp
''')

# Инсерт данных по третьему типу мошенничества

cursor.execute('''
	INSERT INTO s_06_REP_FRAUD(
		event_dt,
		passport,
		fio,
		phone,
		event_type
	)SELECT
		event_dt,
		passport,
		fio,
		phone,
		event_type
	FROM fraud_3_2_tmp
''')

# Инсерт данных по четвертому типу мошенничества

cursor.execute('''
	INSERT INTO s_06_REP_FRAUD(
		event_dt,
		passport,
		fio,
		phone,
		event_type
	)SELECT
		event_dt,
		passport,
		fio,
		phone,
		event_type
	FROM fraud_4_4_tmp
''')

cursor.execute('select * from s_06_REP_FRAUD')
 # # fetchall позволяет считать результат запроса
result = cursor.fetchall()
for row in result:
	print(row)	