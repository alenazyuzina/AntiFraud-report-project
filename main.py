import os
import subprocess
import sqlite3
import jaydebeapi
import sys


connect = jaydebeapi.connect(
'oracle.jdbc.driver.OracleDriver',
'jdbc:oracle:thin:de3hn/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
['de3hn','bilbobaggins'],
'ojdbc7.jar')

cursor = connect.cursor()

sys.path.insert(0, 'py_scripts')

# # Создание стейджинговых таблиц
import terminals_STG
import blacklist_STG
import transactions_STG

# # Переименование и перемещение исходных файлов в архив
import rename

# # Создание таблиц фактов, добавление технических полей
import terminals_DWH
import passport_blacklist_DWH
import transactions_DWH

# # Вычисление мошенничества по первому признаку
import fraud_1_trials

# # Вычисление мошенничества по второму признаку
import fraud_2_trials

# # Вычисление мошенничества по третьему признаку
import fraud_3_trials

# # Вычисление мошенничества по четвертому признаку
import fraud_4_trials

# Создание таблицы отчета
import rep_fraud