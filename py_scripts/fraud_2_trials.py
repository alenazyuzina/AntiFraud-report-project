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
   EXECUTE IMMEDIATE 'DROP TABLE fraud_2_tmp';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;
''')


cursor.execute(''' 
	CREATE TABLE fraud_2_tmp(
		event_dt timestamp,
		passport varchar(128),
		fio varchar (128),
		phone varchar(128),
		event_type varchar(128) default 'Совершение операции при недействующем договоре',
		report_dt timestamp default current_timestamp
	)
''')


cursor.execute('''
	INSERT INTO fraud_2_tmp(
		event_dt,
		passport,
		fio,
		phone
	)SELECT
		transactions.transaction_date,
		clients.passport_num,
		concat(concat(concat(concat(clients.last_name,' '), clients.first_name), ' '), clients.patronymic),
		clients.phone
	FROM s_06_DWH_FACT_transactions  transactions
	INNER JOIN bank.cards cards
	ON transactions.card_num = rtrim(cards.card_num)
	INNER JOIN bank.accounts accounts
	ON cards.account = accounts.account
	INNER JOIN bank.clients clients
	ON accounts.client = clients.client_id
	WHERE transactions.transaction_date > (accounts.valid_to) + 1
	AND transactions.oper_result = 'SUCCESS'
	ORDER BY transactions.transaction_id ASC
''')

# cursor.execute('select count(*) from fraud_2_tmp')

# # fetchall позволяет считать результат запроса
# result = cursor.fetchall()
# for row in result:
# 	print(row)