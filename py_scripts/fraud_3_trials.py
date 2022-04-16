import jaydebeapi
import sqlite3


connect = jaydebeapi.connect(
'oracle.jdbc.driver.OracleDriver',
'jdbc:oracle:thin:de3hn/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
['de3hn','bilbobaggins'],
'ojdbc7.jar')

cursor = connect.cursor()

# cursor.execute('''
# 		SELECT distinct
# 		transactions.transaction_date,
# 		clients.passport_num,
# 		clients.last_name,
# 		clients.first_name,
# 		clients.patronymic,
# 		clients.phone
# 	FROM s_06_STG_transactions  transactions
# 	INNER JOIN s_06_STG_terminals terminals
# 	on transactions.terminal = terminals.terminal_id
# 	INNER JOIN bank.cards cards
# 	on transactions.card_num = rtrim(cards.card_num)
# 	INNER JOIN bank.accounts accounts
#  	on cards.account = accounts.account
#  	INNER JOIN bank.clients
#  	on accounts.client = clients.client_id
# 	where (select count(distinct terminals.terminal_city) from s_06_STG_terminals) > 2
# 	AND transactions.transaction_date between transactions.transaction_date and (transactions.transaction_date + 1/24)
# 	''')


# cursor.execute('''
# 		SELECT distinct
# 		transactions.transaction_date,
# 		clients.last_name,
# 		terminals.terminal_city
# 	FROM s_06_STG_transactions  transactions
# 	INNER JOIN s_06_STG_terminals terminals
# 	on transactions.terminal = terminals.terminal_id
# 	INNER JOIN bank.cards cards
# 	on transactions.card_num = rtrim(cards.card_num)
# 	INNER JOIN bank.accounts accounts
#  	on cards.account = accounts.account
#  	INNER JOIN bank.clients
#  	on accounts.client = clients.client_id
# 	having count(distinct terminals.terminal_city) > 1
# 	-- AND transactions.transaction_date between transactions.transaction_date and (transactions.transaction_date + 1/24)
# 	''')

# cursor.execute('''
# 	SELECT
# 	-- count(distinct transaction_id), extract(hour from transaction_date), 
# 	clients.last_name,
# 		transactions.transaction_date,
# 		clients.passport_num,
# 		clients.first_name,
# 		clients.patronymic,
# 		clients.phone
# 	-- terminals.terminal_city,
# 	-- transactions.transaction_id
# 	FROM s_06_STG_transactions  transactions
# 	INNER JOIN s_06_STG_terminals terminals
#  	on transactions.terminal = terminals.terminal_id
#  	INNER JOIN bank.cards cards
#  	on transactions.card_num = rtrim(cards.card_num)
#  	INNER JOIN bank.accounts accounts
#   	on cards.account = accounts.account
#   	INNER JOIN bank.clients
#   	on accounts.client = clients.client_id
#   	group by extract(hour from transaction_date)
# 	having count(distinct terminals.terminal_city) > 1
# ''')


 # group by clients.last_name, transactions.transaction_date, clients.passport_num, clients.first_name, clients.patronymic,clients.phone
 #  having count(distinct terminals.terminal_city) > 1 

cursor.execute(''' 
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE fraud_3_1_tmp';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;
''')


cursor.execute(''' 
	CREATE TABLE fraud_3_1_tmp(
		transaction_id varchar(128),
		terminal_city varchar(128),
		event_dt timestamp,
		passport varchar(128),
		fio varchar(128),
		account varchar(128),
		phone varchar(128)
	)
''')




cursor.execute('''
	INSERT INTO fraud_3_1_tmp(
		transaction_id,
		terminal_city,
		event_dt,
		passport,
		fio,
		account,
		phone
	)SELECT distinct
		transactions.transaction_id,
		terminals.terminal_city,
		transactions.transaction_date,
		clients.passport_num,
		concat(concat(concat(concat(clients.last_name,' '), clients.first_name), ' '), clients.patronymic),
		accounts.account,
		clients.phone
	FROM s_06_DWH_FACT_transactions  transactions
	inner join s_06_DWH_FACT_terminals terminals
	on transactions.terminal = terminals.terminal_id
	inner join bank.cards cards
	on transactions.card_num = rtrim(cards.card_num)
	inner join bank.accounts accounts
 	on cards.account = accounts.account
 	inner join bank.clients clients
 	on accounts.client = clients.client_id
	''')



# cursor.execute('''
# 			SELECT * FROM fraud_3_1_tmp
# 			where account in(
# 					SELECT 
# 					account
# 				from fraud_3_1_tmp
# 				group by account
# 				having count(distinct terminal_city) > 1
# 				)
# 				''')

# cursor.execute(''' 
# select
#     fio,
#     account,
#     extract(hour from event_dt) hour,
#     count(transaction_id) 
# from fraud_3_1_tmp
# group by extract(hour from event_dt), fio, account
# having count(distinct terminal_city) > 1 
# order by hour
# 	''')


# cursor.execute('''
# 	select
# 	r1.event_dt,
# 	r2.event_dt
# 	from fraud_3_1_tmp r1
# 	cross join fraud_3_1_tmp r2
# 	group by r1.event_dt, r2.event_dt
# 	having r1.event_dt < r2.event_dt +1/24
#  ''')


# cursor.execute('''
# 	select
# 	r1.event_dt,
# 	r2.event_dt,
# 	r1.fio,
# 	r1.transaction_id,
# 	r2.transaction_id,
# 	r1.terminal_city,
# 	r2.terminal_city
# 	from fraud_3_1_tmp r1
# 	cross join fraud_3_1_tmp r2
# 	where r1.account = r2.account
# 	and r1.terminal_city <> r2.terminal_city
# 	and ((r1.event_dt < (r2.event_dt +1/24)) and (r1.event_dt > (r2.event_dt - 1/24)))
#  ''')


# cursor.execute('''
	# select distinct
	# r1.event_dt,
	# r2.event_dt,
	# r1.fio,
	# r1.passport,
	# r1.transaction_id,
	# r2.transaction_id,
	# r1.terminal_city,
	# r2.terminal_city
	# from fraud_3_1_tmp r1
	# cross join fraud_3_1_tmp r2
	# where r1.account = r2.account
	# and r1.terminal_city <> r2.terminal_city
	# and ((r1.event_dt < r2.event_dt) and (r1.event_dt > (r2.event_dt - 1/24)))
 # ''')


cursor.execute(''' 
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE fraud_3_2_tmp';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;
''')


cursor.execute('''
	CREATE TABLE fraud_3_2_tmp(
		event_dt timestamp,
		passport varchar(128),
		fio varchar(128),
		phone varchar(128),
		event_type varchar(128) default 'Совершение операций в разных городах в течение одного часа',
		report_dt timestamp default current_timestamp
	)
''')

cursor.execute(''' 
	INSERT INTO fraud_3_2_tmp(
		event_dt,
		passport,
		fio,
		phone
	) SELECT distinct
		r2.event_dt,
		r1.passport,
		r1.fio,
		r1.phone
	from fraud_3_1_tmp r1
	cross join fraud_3_1_tmp r2
	where r1.account = r2.account
	and r1.terminal_city <> r2.terminal_city
	and ((r1.event_dt < r2.event_dt) and (r1.event_dt > (r2.event_dt - 1/24)))
 ''')

cursor.execute('select * from fraud_3_2_tmp')
result = cursor.fetchall()
for row in result:
	print(row)

