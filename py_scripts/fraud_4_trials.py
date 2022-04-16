import jaydebeapi
import sqlite3


connect = jaydebeapi.connect(
'oracle.jdbc.driver.OracleDriver',
'jdbc:oracle:thin:de3hn/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
['de3hn','bilbobaggins'],
'ojdbc7.jar')

cursor = connect.cursor()



# Создание промежуточной таблицы с транзакциями + терминалами

cursor.execute(''' 
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE fraud_4_1_tmp';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;
''')


cursor.execute(''' 
   CREATE TABLE fraud_4_1_tmp(
      transaction_id varchar(128),
      terminal_city varchar(128),
      event_dt timestamp,
      passport varchar(128),
      fio varchar(128),
      account varchar(128),
      phone varchar(128),
      oper_result varchar(128),
      amount decimal
   )
''')




cursor.execute('''
   INSERT INTO fraud_4_1_tmp(
      transaction_id,
      terminal_city,
      event_dt,
      passport,
      fio,
      account,
      phone,
      oper_result,
      amount
   )SELECT distinct
      transactions.transaction_id,
      terminals.terminal_city,
      transactions.transaction_date,
      clients.passport_num,
      concat(concat(concat(concat(clients.last_name,' '), clients.first_name), ' '), clients.patronymic),
      accounts.account,
      clients.phone,
      transactions.oper_result,
      transactions.amount
   FROM s_06_DWH_FACT_transactions  transactions
   INNER JOIN s_06_DWH_FACT_terminals terminals
   on transactions.terminal = terminals.terminal_id
   INNER JOIN bank.cards cards
   on transactions.card_num = rtrim(cards.card_num)
   INNER JOIN bank.accounts accounts
   on cards.account = accounts.account
   INNER JOIN bank.clients clients
   on accounts.client = clients.client_id
   ''')


# Создание промежуточной таблицы с транзакциями + терминалами с условием наличия REJECT в цепочке
cursor.execute(''' 
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE fraud_4_2_tmp';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;
''')


cursor.execute(''' 
   CREATE TABLE fraud_4_2_tmp(
      transaction_id varchar(128),
      terminal_city varchar(128),
      event_dt timestamp,
      passport varchar(128),
      fio varchar(128),
      account varchar(128),
      phone varchar(128),
      oper_result varchar(128),
      amount decimal
   )
''')

cursor.execute('''
   INSERT INTO fraud_4_2_tmp(
      transaction_id,
      terminal_city,
      event_dt,
      passport,
      fio,
      account,
      phone,
      oper_result,
      amount
   ) SELECT
      transaction_id,
      terminal_city,
      event_dt,
      passport,
      fio,
      account,
      phone,
      oper_result,
      amount
   FROM fraud_4_1_tmp
   WHERE account in (select account from fraud_4_1_tmp where oper_result = 'REJECT')
''')


# Создание промежуточной таблицы с серией из четырех транзакций
cursor.execute(''' 
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE fraud_4_3_tmp';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;
''')


cursor.execute(''' 
   CREATE TABLE fraud_4_3_tmp(
   transaction_id_1 varchar(128),
   oper_result_1 varchar(128),
   amount_1 decimal,
   transaction_id_2 varchar(128),
   oper_result_2 varchar(128),
   amount_2 decimal,
   transaction_id_3 varchar(128),
   oper_result_3 varchar(128),
   amount_3 decimal,
   transaction_id_4 varchar(128),
   oper_result_4 varchar(128),
   amount_4 decimal,
   event_dt_1 timestamp,
   event_dt_4 timestamp
   )
''')


cursor.execute(''' 
   INSERT INTO fraud_4_3_tmp(
         transaction_id_1,
         oper_result_1,
         amount_1,
         transaction_id_2,
         oper_result_2,
         amount_2,
         transaction_id_3,
         oper_result_3,
         amount_3,
         transaction_id_4,
         oper_result_4,
         amount_4,
         event_dt_1,
         event_dt_4
   ) SELECT
      transaction_id,
      oper_result,
      amount,
      lead(transaction_id) over(partition by account order by transaction_id),
      lead(oper_result) over(partition by account order by transaction_id),
      lead(amount) over(partition by account order by transaction_id),
      lead(transaction_id, 2) over(partition by account order by transaction_id),
      lead(oper_result, 2) over(partition by account order by transaction_id),
      lead(amount, 2) over(partition by account order by transaction_id),
      lead(transaction_id, 3) over(partition by account order by transaction_id),
      lead(oper_result, 3) over(partition by account order by transaction_id),
      lead(amount,3) over(partition by account order by transaction_id),
      event_dt,
      lead(event_dt, 3) over(partition by account order by transaction_id)
   FROM fraud_4_2_tmp
''')

cursor.execute('''
      SELECT
         transaction_id,
         oper_result,
         amount,
         lead(transaction_id) over(partition by account order by transaction_id),
         lead(oper_result) over(partition by account order by transaction_id),
         lead(amount) over(partition by account order by transaction_id),
         lead(transaction_id, 2) over(partition by account order by transaction_id),
         lead(oper_result, 2) over(partition by account order by transaction_id),
         lead(amount, 2) over(partition by account order by transaction_id),
         lead(transaction_id, 3) over(partition by account order by transaction_id),
         lead(oper_result, 3) over(partition by account order by transaction_id),
         lead(amount,3) over(partition by account order by transaction_id)
      FROM fraud_4_2_tmp
 ''')

cursor.execute('''
SELECT * FROM fraud_4_3_tmp
WHERE oper_result_1 = 'REJECT'
AND oper_result_2 = 'REJECT'
AND oper_result_3 = 'REJECT'
AND oper_result_4 = 'SUCCESS'
AND amount_1 > amount_2
AND amount_2 > amount_3
AND amount_3 > amount_4
AND event_dt_1 > event_dt_4 - (20/1440)
 ''')

cursor.execute('''select 
                     * 
                  from fraud_4_1_tmp 
                  where transaction_id in (
                     SELECT transaction_id_4 FROM fraud_4_3_tmp
                  WHERE oper_result_1 = 'REJECT'
                  AND oper_result_2 = 'REJECT'
                  AND oper_result_3 = 'REJECT'
                  AND oper_result_4 = 'SUCCESS'
                  AND amount_1 > amount_2
                  AND amount_2 > amount_3
                  AND amount_3 > amount_4
                  AND event_dt_1 > event_dt_4 - (20/1440)
                  )
               ''')

# Создание промежуточной таблицы с необходимыми полями

cursor.execute(''' 
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE fraud_4_4_tmp';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;
''')



cursor.execute('''
   CREATE TABLE fraud_4_4_tmp(
      event_dt timestamp,
      passport varchar(128),
      fio varchar(128),
      phone varchar(128),
      event_type varchar(128) default 'Попытка подбора суммы',
      report_dt timestamp default current_timestamp
   ) 
   ''')
cursor.execute(''' 
   INSERT INTO fraud_4_4_tmp(
   event_dt,
   passport,
   fio,
   phone
   )select 
      event_dt,
      passport,
      fio,
      phone 
   from fraud_4_1_tmp 
   where transaction_id in (
                      SELECT transaction_id_4 FROM fraud_4_3_tmp
                      WHERE oper_result_1 = 'REJECT'
                      AND oper_result_2 = 'REJECT'
                      AND oper_result_3 = 'REJECT'
                      AND oper_result_4 = 'SUCCESS'
                      AND amount_1 > amount_2
                      AND amount_2 > amount_3
                      AND amount_3 > amount_4
                      AND event_dt_1 > event_dt_4 - (20/1440)
                      )
   ''')


# cursor.execute('select * from fraud_4_4_tmp')
# result = cursor.fetchall()
# for row in result:
#  print(row)

