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
   EXECUTE IMMEDIATE 'DROP TABLE fraud_1_tmp';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;
''')

cursor.execute(''' 
	CREATE TABLE fraud_1_tmp(
		event_dt timestamp,
		passport varchar(128),
		fio varchar (128),
		phone varchar(128),
		event_type varchar(128) default 'Совершение операции при просроченном или заблокированном паспорте',
		report_dt timestamp default current_timestamp
	)
''')

# функция для добавления просроченного паспорта
cursor.execute('''
	INSERT INTO fraud_1_tmp(
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
	INNER JOIN  bank.accounts accounts
	ON cards.account = accounts.account
	INNER JOIN  bank.clients clients
	ON accounts.client = clients.client_id
	WHERE transactions.transaction_date > (clients.passport_valid_to) +1
	AND transactions.oper_result = 'SUCCESS'
	ORDER BY transaction_id ASC 
''')



# функция для добавления паспорта из блэклиста
cursor.execute(''' 
	INSERT INTO fraud_1_tmp(
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
	INNER JOIN s_06_DWH_FACT_pssprt_blcklst blacklist
	ON clients.passport_num = blacklist.passport
	WHERE clients.passport_num = blacklist.passport
	AND transactions.transaction_date > (blacklist.entry_dt) + 1
	AND transactions.oper_result = 'SUCCESS'
''')

# cursor.execute('select count(*) from fraud_1_tmp')
# # # fetchall позволяет считать результат запроса
# result = cursor.fetchall()
# for row in result:
# 	print(row)